#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify — 中国法律法规条文核验 MCP server。

定位：给 AI 的法条核验层。从官方源（国家法律法规数据库 flk.npc.gov.cn）
取回法规与条文原文，并对给定引文做代码层核验（存在性 / 逐字比对 /
效力状态三检）。核验逻辑是确定性代码，不经模型。8 个工具全部只读：

    flk_search / get_law / get_article / search_articles   检索与取原文
    verify_citation / check_citations_in_text              引注核验
    lookup                                                 一句话直达
    check_support                                          蕴含初筛（启发式）

双通道：本地快照库（SQLite，见 tools/build_snapshot.py）优先保证稳定，
官方实时通道复核最新状态；结论出参恒标注通道与数据时间，永不混写。

设计纪律（继承 flk-mcp 的经验、补其缺点，详见 docs/DESIGN.md）：
- 数据只来自官方源，请求只发往白名单域名，且只走 HTTPS；
- 下载条文时全程开启 TLS 证书校验，跟随重定向逐跳复检目标域名
  （flk-mcp 在此处关掉过证书校验，本项目将其作为回归红线修复）；
- 实时通道单次查询不落盘；发往官方源的只有本次检索词本身；
- 遇验证码/拦截页立即终止并如实报错，绝不用旧数据冒充实时结果；
- 目标域名不可经环境变量改写（环境变量只许调超时与本地路径）。

零依赖：纯 Python 3.10+ 标准库，手写 MCP JSON-RPC 闭环。
传输：stdio ＋ Streamable HTTP（同一核验内核、同一 handle_request）。
协议版本策略：按 2025-03-26 / 2025-11-25 的会话式语义实现，
同时兼容 2024-11-05；请求处理保持无状态。

用法：
    python3 cn_law_verify_server.py                    # stdio 模式
    python3 cn_law_verify_server.py --http --port 8765 # Streamable HTTP
    python3 cn_law_verify_server.py --version
"""

import difflib
import gzip
import http.client
import io
import json
import os
import re
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

SERVER_NAME = "cn-law-verify"
SERVER_VERSION = "0.2.0"
RESULT_SCHEMA = "cn-law-verify/result@1"
SUPPORTED_PROTOCOLS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
DEFAULT_PROTOCOL = "2025-03-26"

FLK_BASE = "https://flk.npc.gov.cn"
# API 请求只许发往这两个官方域名；下载跳转另许联通云对象存储域名后缀
# （flk 官方 DOCX 的签名下载地址所在域名），且逐跳复检。
API_HOSTS = {"flk.npc.gov.cn", "wb.flk.npc.gov.cn"}
DOWNLOAD_HOST_SUFFIXES = (".cucloud.cn",)
USER_AGENT = "cn-law-verify/0.2.0 (+https://github.com/songoao25/cn-law-verify)"

TIMEOUT = float(os.environ.get("CN_LAW_VERIFY_TIMEOUT", "30"))
MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024      # 下载上限 50MB
MAX_ZIP_ENTRIES = 5000                      # DOCX 解压文件数上限
MAX_ZIP_UNCOMPRESSED = 100 * 1024 * 1024    # DOCX 解压总量上限 100MB
MAX_REDIRECTS = 5

# 官方 sxx 效力代码 → 状态（接口事实：3=有效、2=已修改、1=已废止、4=尚未生效）
STATUS_MAP = {
    3: ("有效", "effective"),
    2: ("已修改", "amended"),
    1: ("已废止", "repealed"),
    4: ("尚未生效", "not_yet_effective"),
}

BLOCK_HINTS = ("验证码", "滑动验证", "访问验证", "安全验证", "captcha")


class SourceError(Exception):
    """官方源不可用/被拦截/响应异常——如实上报，绝不静默降级。"""


def log(*args):
    print("[cn-law-verify]", *args, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# HTTP 层：白名单 + HTTPS + TLS 全程校验 + 重试
# ---------------------------------------------------------------------------

def _check_url(url, *, for_download=False):
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != "https":
        raise SourceError(f"拒绝非 HTTPS 地址：{url}")
    host = parts.hostname or ""
    if host in API_HOSTS:
        return
    if for_download and host.endswith(DOWNLOAD_HOST_SUFFIXES):
        return
    raise SourceError(f"拒绝白名单之外的目标域名：{host}")


class _ManualRedirect(urllib.request.HTTPRedirectHandler):
    """不自动跟随重定向，交由调用方逐跳复检域名后再跟随。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _open_once(url, data=None, headers=None, *, for_download=False,
               gzip_ok=False):
    _check_url(url, for_download=for_download)
    req = urllib.request.Request(url, data=data,
                                 method="POST" if data is not None else "GET")
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Referer", "https://flk.npc.gov.cn/index.html")
    if gzip_ok:
        # JSON 接口响应压缩后体积降一个量级，显著减少传输被截断的概率
        req.add_header("Accept-Encoding", "gzip")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    opener = urllib.request.build_opener(
        _ManualRedirect(),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
    )
    return opener.open(req, timeout=TIMEOUT)


def _read_body(resp, limit=None, expected=None):
    chunks, total = [], 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if limit is not None and total > limit:
            raise SourceError("响应体积超过上限，已中止读取")
        chunks.append(chunk)
    if expected is not None and total != expected:
        # 代理/网络可能静默截断响应体（无异常但字节数不足），必须按失败重试，
        # 绝不把截断的数据当完整结果用。
        raise IOError(f"响应被截断：期望 {expected} 字节，实得 {total} 字节")
    return b"".join(chunks)


def _request(url, data=None, headers=None, *, for_download=False,
             body_limit=None, retries=5, gzip_ok=False):
    """发请求并读完响应体；3xx 时逐跳复检域名后跟随；失败重试。"""
    last_err = None
    for attempt in range(retries):
        try:
            current, payload, hdrs = url, data, headers
            for _ in range(MAX_REDIRECTS + 1):
                try:
                    resp = _open_once(current, payload, hdrs,
                                      for_download=for_download,
                                      gzip_ok=gzip_ok)
                except urllib.error.HTTPError as e:
                    if e.code in (301, 302, 303, 307, 308):
                        location = e.headers.get("Location")
                        if not location:
                            raise SourceError(f"官方源返回 {e.code} 但无跳转地址")
                        nxt = urllib.parse.urljoin(current, location)
                        _check_url(nxt, for_download=for_download)
                        current, payload, hdrs = nxt, None, None
                        continue
                    body = e.read().decode("utf-8", "replace") if e.fp else ""
                    raise SourceError(f"官方源 HTTP {e.code}：{body[:200]}")
                with resp:
                    if resp.status in (301, 302, 303, 307, 308):
                        location = resp.headers.get("Location")
                        if not location:
                            raise SourceError("官方源跳转但无目标地址")
                        nxt = urllib.parse.urljoin(current, location)
                        _check_url(nxt, for_download=for_download)
                        current, payload, hdrs = nxt, None, None
                        continue
                    expected = resp.headers.get("Content-Length")
                    body = _read_body(
                        resp, limit=body_limit,
                        expected=int(expected) if expected else None)
                    if gzip_ok and (resp.headers.get("Content-Encoding")
                                    or "").lower() == "gzip":
                        body = gzip.decompress(body)
                    return body
            raise SourceError("重定向次数过多")
        except SourceError:
            raise
        except Exception as e:  # 网络抖动/连接重置/读流截断 → 重试
            last_err = e
            log(f"请求失败（第 {attempt + 1} 次）：{e!r}")
            time.sleep(1.0 * (attempt + 1))
    raise SourceError(f"官方源请求多次失败：{last_err!r}")


def _http_json(path, body=None):
    url = FLK_BASE + path
    data = None
    headers = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json;charset=UTF-8"}
    raw = _request(url, data, headers, gzip_ok=True)
    text = raw.decode("utf-8", "replace")
    for hint in BLOCK_HINTS:
        if hint in text:
            raise SourceError("官方源返回了验证/拦截页，已终止查询；"
                              "请稍后再试，或打开官方链接人工核验")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise SourceError(f"官方源响应不是合法 JSON：{text[:200]}")


# ---------------------------------------------------------------------------
# flk 官方接口（接口事实来自公开前端行为，独立实现）
# ---------------------------------------------------------------------------

def flk_search_rows(query, search_type=2, search_range=1, page_size=10):
    """POST /law-search/search/list → (total, rows)。"""
    body = {
        "searchContent": query,
        "searchType": search_type,      # 1=精确 2=模糊
        "searchRange": search_range,    # 1=标题 2=正文
        "pageNum": 1,
        "pageSize": max(1, min(int(page_size), 20)),
        "sxx": [], "gbrq": [], "sxrq": [],
        "flfgCodeId": [], "zdjgCodeId": [],
    }
    res = _http_json("/law-search/search/list", body)
    if not isinstance(res, dict) or "rows" not in res:
        raise SourceError(f"检索响应结构异常：{str(res)[:200]}")
    return int(res.get("total") or 0), res.get("rows") or []


_TAG_RE = re.compile(r"<[^>]+>")


def clean_title(raw):
    return _TAG_RE.sub("", raw or "").strip()


def map_status(code):
    try:
        code = int(code)
    except (TypeError, ValueError):
        return ("未知", "unknown", code)
    zh, en = STATUS_MAP.get(code, ("未知", "unknown"))
    return (zh, en, code)


def row_to_result(row):
    status_zh, status_en, code = map_status(row.get("sxx"))
    bbbs = row.get("bbbs") or ""
    return {
        "id": bbbs,
        "title": clean_title(row.get("title")),
        "authority": row.get("zdjgName") or "",
        "category": row.get("flxz") or "",
        "promulgated": row.get("gbrq") or "",
        "effective_from": row.get("sxrq") or "",
        "status": status_zh,
        "status_en": status_en,
        "status_code": code,
        "source_url": f"https://flk.npc.gov.cn/detail.html?bbbs={bbbs}",
    }


_STATUS_PREFERENCE = {3: 0, 2: 1, 4: 2, 1: 3}


def find_law(law_name):
    """按法名精确定位一部法规：同名多版本时优先现行有效版。

    返回 (result_dict, match)；match ∈ {"exact", "contains", None}。
    """
    name = (law_name or "").strip().strip("《》").strip()
    if not name:
        raise SourceError("法名为空")
    _, rows = flk_search_rows(name, search_type=1, search_range=1, page_size=10)
    results = [row_to_result(r) for r in rows]
    exact = [r for r in results if r["title"] == name]
    if exact:
        exact.sort(key=lambda r: _STATUS_PREFERENCE.get(r["status_code"], 9))
        return exact[0], "exact"
    # 简称/包含关系才算定位成功；仅「沾边」的第一条结果绝不冒充命中——
    # 否则编造的法名会被随便一部法规顶替，存在性核验就成了假的。
    contains = [r for r in results
                 if (name and name in r["title"]) or r["title"] in name]
    if contains:
        contains.sort(key=lambda r: (_STATUS_PREFERENCE.get(r["status_code"], 9),
                                     abs(len(r["title"]) - len(name))))
        return contains[0], "contains"
    return None, None


def _resolve_download_url(start_url):
    """逐跳复检域名跟随 download/mobile 的跳转，返回最终下载地址。

    连接层异常（远端断连等）整段重试 3 次后才放弃。
    """
    last_err = None
    for _attempt in range(3):
        try:
            current = start_url
            for _ in range(MAX_REDIRECTS + 1):
                _check_url(current, for_download=True)
                try:
                    resp = _open_once(current, for_download=True)
                except urllib.error.HTTPError as e:
                    if e.code in (301, 302, 303, 307, 308):
                        location = e.headers.get("Location")
                        if not location:
                            raise SourceError(
                                f"官方源返回 {e.code} 但无跳转地址")
                        current = urllib.parse.urljoin(current, location)
                        continue
                    raise SourceError(f"官方源 HTTP {e.code}")
                with resp:
                    if resp.status in (301, 302, 303, 307, 308):
                        location = resp.headers.get("Location")
                        if not location:
                            raise SourceError("官方源跳转但无目标地址")
                        current = urllib.parse.urljoin(current, location)
                        continue
                    return current
            raise SourceError("重定向次数过多")
        except SourceError:
            raise
        except (OSError, http.client.HTTPException,
                urllib.error.URLError) as e:
            last_err = e
            log(f"解析下载地址连接失败（第 {_attempt + 1} 次）：{e!r}")
            time.sleep(1.0 * (_attempt + 1))
    raise SourceError(f"解析下载地址多次失败：{last_err!r}")


def _download_docx_bytes(bbbs):
    """断点续传下载官方 DOCX。

    本机出口对大响应常静默截断：整文件重下会反复断在同一量级。
    改法：先解出签名下载地址，再以 Range 头逐段续拉；读流中断时
    IncompleteRead 携带的部分字节也保留进缓冲。签名地址不支持
    Range（403/416）时回退到整文件重试路径。
    """
    start_url = (f"{FLK_BASE}/law-search/download/mobile"
                 f"?bbbs={urllib.parse.quote(bbbs)}&format=docx")
    final_url = _resolve_download_url(start_url)
    buf = bytearray()
    total = None
    last_err = None
    for attempt in range(14):
        headers = {"Range": f"bytes={len(buf)}-"} if buf else None
        try:
            resp = _open_once(final_url, headers=headers, for_download=True)
        except urllib.error.HTTPError as e:
            if e.code in (403, 416) and buf:
                log("签名地址不支持 Range，回退整文件下载")
                return _request(start_url, for_download=True,
                                body_limit=MAX_DOWNLOAD_BYTES, retries=5)
            if e.code in (301, 302, 303, 307, 308):
                final_url = _resolve_download_url(start_url)
                continue
            raise SourceError(f"官方源 HTTP {e.code}")
        except (OSError, http.client.HTTPException,
                urllib.error.URLError) as e:
            # 连接层失败（HTTPError 已在上方先行处理）：已下字节保留续传
            last_err = e
            log(f"DOCX 续传连接失败（第 {attempt + 1} 段）：{e!r}")
            time.sleep(0.8)
            continue
        clean_eof = False
        with resp:
            if resp.status == 200 and buf:
                buf = bytearray()  # 服务器忽略 Range，整文件重读
            elif resp.status not in (200, 206):
                raise SourceError(f"下载返回异常状态 {resp.status}")
            cr = resp.headers.get("Content-Range") or ""
            m = re.search(r"/(\d+)$", cr)
            if m:
                total = int(m.group(1))
            elif resp.status == 200:
                cl = resp.headers.get("Content-Length")
                if cl:
                    total = int(cl)
            try:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        clean_eof = True
                        break
                    buf.extend(chunk)
                    if len(buf) > MAX_DOWNLOAD_BYTES:
                        raise SourceError("下载体积超过上限")
            except http.client.IncompleteRead as e:
                if e.partial:
                    buf.extend(e.partial)
            except (OSError, http.client.HTTPException) as e:
                last_err = e
        if total is not None and len(buf) >= total:
            return bytes(buf[:total])
        if total is None and clean_eof and buf:
            return bytes(buf)
        log(f"DOCX 续传中：已得 {len(buf)}/{total or '?'} 字节"
            f"（第 {attempt + 1} 段）")
        time.sleep(0.8)
    raise SourceError(f"DOCX 下载多次续传仍不完整：期望 {total} 字节，"
                      f"实得 {len(buf)} 字节（末次错误 {last_err!r}）")


def fetch_law_docx_paragraphs(bbbs):
    """下载官方 DOCX 全文并解析为段落文本列表（TLS 全程校验、逐跳复检）。"""
    raw = _download_docx_bytes(bbbs)
    return docx_to_paragraphs(raw)


_W_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def docx_to_paragraphs(raw):
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise SourceError("官方 DOCX 下载内容不是合法的 zip/docx 文件")
    infos = zf.infolist()
    if len(infos) > MAX_ZIP_ENTRIES:
        raise SourceError("DOCX 内文件数超过安全上限")
    if sum(i.file_size for i in infos) > MAX_ZIP_UNCOMPRESSED:
        raise SourceError("DOCX 解压体积超过安全上限")
    try:
        xml_bytes = zf.read("word/document.xml")
    except KeyError:
        raise SourceError("DOCX 缺少 word/document.xml")
    root = ET.fromstring(xml_bytes)
    paragraphs = []
    for p in root.iter(f"{_W_NS}p"):
        text = "".join(t.text or "" for t in p.iter(f"{_W_NS}t"))
        text = text.replace("　", " ").strip()
        if text:
            paragraphs.append(text)
    if not paragraphs:
        raise SourceError("DOCX 未解析出任何段落")
    return paragraphs


def fetch_hit_windows(bbbs, keyword):
    """POST /law-search/search/hitDisplay → 关键词上下文窗口（兜底通道）。"""
    body = {
        "bbbs": bbbs,
        "searchContent": keyword,
        "searchType": 2,
        "searchRange": 2,
        "pageNum": 1,
        "pageSize": 10,
        "sxx": [], "gbrq": [], "sxrq": [],
        "flfgCodeId": [], "zdjgCodeId": [],
    }
    res = _http_json("/law-search/search/hitDisplay", body)
    data = res.get("data") or {}
    windows = data.get("contentHitDisplay") or []
    return [clean_title(w) for w in windows]


def fetch_law_details(bbbs):
    """GET /law-search/search/flfgDetails → 元数据＋目录树＋版本沿革。

    接口事实（2026-11-23 实测）：data 含 title/zdjgName/flxz/gbrq/sxrq/sxx
    等元数据；content 为目录树（节点 title＋children，叶节点细到条）；
    lsyg 为版本沿革列表 [{bbbs, title, gbrq, highLight}]（无沿革时为
    None 或空）；xgzl 为相关资料。content 本身不含条文正文。
    """
    res = _http_json("/law-search/search/flfgDetails"
                     f"?bbbs={urllib.parse.quote(bbbs)}")
    data = res.get("data")
    if not isinstance(data, dict) or not data.get("title"):
        raise SourceError(f"详情响应结构异常：{str(res)[:200]}")
    return data


def flatten_toc(node, depth=0, out=None):
    """把 flfgDetails 的目录树压平为 [{title, depth}]，并统计条数。"""
    if out is None:
        out = []
    if not isinstance(node, dict):
        return out
    title = (node.get("title") or "").strip()
    if title:
        out.append({"title": title, "depth": depth})
    for child in node.get("children") or []:
        flatten_toc(child, depth + 1, out)
    return out


_ARTICLE_TITLE_ONLY_RE = re.compile(
    r"^第[零一二三四五六七八九十百千]+条(?:之[零一二三四五六七八九十]+)?$")


def toc_article_count(flat_toc):
    return sum(1 for item in flat_toc
               if _ARTICLE_TITLE_ONLY_RE.match(item["title"]))


# ---------------------------------------------------------------------------
# 条号归一化与条文抽取（确定性代码，不经模型）
# ---------------------------------------------------------------------------

_CN_DIGITS = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
              "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
_CN_UNITS = {"十": 10, "百": 100, "千": 1000}


def cn_to_int(s):
    """中文数字 → 整数（支持到千位，够法条编号用）。"""
    s = (s or "").strip()
    if not s:
        raise ValueError("空数字")
    if s.isdigit():
        return int(s)
    total, current = 0, 0
    for ch in s:
        if ch in _CN_DIGITS:
            current = _CN_DIGITS[ch]
        elif ch in _CN_UNITS:
            unit = _CN_UNITS[ch]
            total += (current or 1) * unit
            current = 0
        else:
            raise ValueError(f"无法解析的中文数字：{s}")
    return total + current


def int_to_cn(n):
    """整数 → 中文数字（法条标题写法，如 577 → 五百七十七）。"""
    if n == 0:
        return "零"
    digits = "零一二三四五六七八九"
    units = ["", "十", "百", "千"]
    s = str(int(n))
    out = []
    for i, ch in enumerate(s):
        d = int(ch)
        unit = units[len(s) - 1 - i]
        if d == 0:
            if out and out[-1] != "零":
                out.append("零")
        else:
            out.append(digits[d] + unit)
    text = "".join(out).rstrip("零")
    if text.startswith("一十"):
        text = text[1:]
    return text


_ARTICLE_INPUT_RE = re.compile(
    r"^第?\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?$")


def normalize_article(article):
    """条号入参归一化 → (条数 int, 之几 int|None)。

    接受：577 / "577" / "第577条" / "第五百七十七条" / "第五百七十七条之一"。
    """
    if isinstance(article, int):
        return article, None
    s = str(article or "").strip()
    m = _ARTICLE_INPUT_RE.match(s)
    if not m:
        raise ValueError(f"无法解析的条号：{article!r}")
    num = cn_to_int(m.group(1))
    sub = cn_to_int(m.group(2)) if m.group(2) else None
    return num, sub


def article_heading(num, sub=None):
    heading = f"第{int_to_cn(num)}条"
    if sub:
        heading += f"之{int_to_cn(sub)}"
    return heading


_ARTICLE_HEAD_RE = re.compile(
    r"^第([零一二三四五六七八九十百千]+)条"
    r"(?:之([零一二三四五六七八九十]+))?(?=[\s　]|$|[^条])")
_SECTION_HEAD_RE = re.compile(r"^第[零一二三四五六七八九十百千]+[章节编]")


def extract_article(paragraphs, num, sub=None):
    """从全文段落中整条截取指定条文；找不到返回 None。"""
    target = article_heading(num, sub)
    start = None
    for i, para in enumerate(paragraphs):
        m = _ARTICLE_HEAD_RE.match(para)
        if m:
            try:
                n = cn_to_int(m.group(1))
                s = cn_to_int(m.group(2)) if m.group(2) else None
            except ValueError:
                continue
            if n == num and s == sub:
                start = i
                break
    if start is None:
        return None
    collected = []
    first = paragraphs[start]
    m = _ARTICLE_HEAD_RE.match(first)
    rest = first[m.end():].strip()
    if rest:
        collected.append(rest)
    for para in paragraphs[start + 1:]:
        if _ARTICLE_HEAD_RE.match(para) or _SECTION_HEAD_RE.match(para):
            break
        if para.startswith(("附件", "附则")) and not collected:
            break
        collected.append(para)
    return {"heading": target, "text": "\n".join(collected).strip()}


def extract_article_from_text(text, num, sub=None):
    """在纯文本（如 hitDisplay 上下文窗口拼接文本）中按条号截取条文段。

    窗口文本是多个命中片段的拼接，必须先定位目标条标题、截到下一条
    标题为止，不能拿整段拼接文本冒充该条原文。
    """
    if not text:
        return None
    target = article_heading(num, sub)
    # 窗口文本中标题可能出现在任意位置（非行首），逐处匹配标题字面
    pattern = re.compile(re.escape(target) + r"(?=[\s　]|$|[^条])")
    m = pattern.search(text)
    if not m:
        return None
    rest = text[m.end():]
    nxt = re.search(r"第[零一二三四五六七八九十百千]+条(?:之[零一二三四五六七八九十]+)?(?=[\s　])",
                    rest)
    if nxt:
        rest = rest[:nxt.start()]
    rest = rest.strip()
    return {"heading": target, "text": rest} if rest else None


def split_articles(paragraphs):
    """把法规全文段落切成条文列表 [{num, sub, heading, text}]。

    段落以条标题开头即起一条；标题后同段文字与后续段落都归该条，
    直到下一条标题或编/章/节标题。同号重复出现时（目录页与正文各一份
    等情形）保留文本更长的一份。第一条之前的前言段落不计入。
    """
    segments = []
    current = None
    for para in paragraphs:
        m = _ARTICLE_HEAD_RE.match(para)
        if m:
            try:
                num = cn_to_int(m.group(1))
                sub = cn_to_int(m.group(2)) if m.group(2) else None
            except ValueError:
                num = sub = None
            if num is not None:
                if current:
                    segments.append(current)
                rest = para[m.end():].strip()
                current = {"num": num, "sub": sub,
                           "heading": article_heading(num, sub),
                           "parts": [rest] if rest else []}
                continue
        if _SECTION_HEAD_RE.match(para):
            if current:
                segments.append(current)
                current = None
            continue
        if current is not None:
            current["parts"].append(para)
    if current:
        segments.append(current)
    best = {}
    order = []
    for seg in segments:
        text = "\n".join(p for p in seg["parts"] if p).strip()
        item = {"num": seg["num"], "sub": seg["sub"],
                "heading": seg["heading"], "text": text}
        key = (seg["num"], seg["sub"] or 0)
        if key not in best:
            order.append(key)
            best[key] = item
        elif len(text) > len(best[key]["text"]):
            best[key] = item
    return [best[k] for k in order]


_CITATION_RE = re.compile(
    r"《([^》]{2,60})》\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")


def extract_citations(text):
    """从整段文本中抽取《法名》第N条形式的引注（P2 供 check 工具用）。"""
    out = []
    for m in _CITATION_RE.finditer(text or ""):
        try:
            num = cn_to_int(m.group(2))
            sub = cn_to_int(m.group(3)) if m.group(3) else None
        except ValueError:
            continue
        out.append({"law": m.group(1), "article_num": num,
                    "article_sub": sub,
                    "article": article_heading(num, sub),
                    "span": [m.start(), m.end()]})
    return out


# ---------------------------------------------------------------------------
# 工具实现
# ---------------------------------------------------------------------------

def _snapshot(channel):
    return {
        "channel": channel,
        "source": "国家法律法规数据库 flk.npc.gov.cn（官方源实时查询）",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def tool_flk_search(args):
    query = (args.get("query") or "").strip()
    if not query:
        raise SourceError("query 不能为空")
    mode = args.get("mode", "fuzzy")
    scope = args.get("scope", "title")
    limit = args.get("limit", 10)
    total, rows = flk_search_rows(
        query,
        search_type=1 if mode == "exact" else 2,
        search_range=2 if scope == "body" else 1,
        page_size=limit,
    )
    return {
        "schema": RESULT_SCHEMA,
        "tool": "flk_search",
        "query": query,
        "total": total,
        "results": [row_to_result(r) for r in rows],
        "data_snapshot": _snapshot("live"),
    }


def _normalize_for_compare(text):
    return re.sub(r"\s+", "", (text or "")).replace("　", "")


def _docx_paragraphs_cached(bbbs, cache=None):
    """取官方 DOCX 全文段落；cache 为 dict 时在单次工具调用内复用。

    缓存只活在一次工具调用期间（整段核验逐条查同一部法时避免重复
    下载整部 DOCX），不落盘、不跨调用——实时通道「不留存」纪律不变。
    """
    if cache is not None and bbbs in cache:
        cached = cache[bbbs]
        if isinstance(cached, Exception):
            raise cached
        return cached
    try:
        paragraphs = fetch_law_docx_paragraphs(bbbs)
    except SourceError as e:
        if cache is not None:
            cache[bbbs] = e
        raise
    if cache is not None:
        cache[bbbs] = paragraphs
    return paragraphs


def fetch_official_article(law, num, sub, cache=None):
    """实时通道取单条官方原文 → (text, text_source, notes)。

    优先官方 DOCX 全文整条截取；不可用时降级 hitDisplay 上下文窗口
    并如实标注来源（窗口文本可能被截断，调用方须据此降级结论）。
    """
    notes = []
    heading = article_heading(num, sub)
    try:
        paragraphs = _docx_paragraphs_cached(law["id"], cache)
        found = extract_article(paragraphs, num, sub)
        if found:
            return found["text"], "official_docx_full_text", notes
    except SourceError as e:
        notes.append(f"官方全文（DOCX）获取失败，转入上下文窗口兜底：{e}")
    windows = fetch_hit_windows(law["id"], heading)
    segment = extract_article_from_text("\n".join(windows), num, sub)
    if segment:
        notes.append("原文取自官方检索的上下文窗口中截取的条文段（非全文），"
                     "长条文可能被截断")
        return segment["text"], "official_context_window", notes
    return None, None, notes


def tool_verify_citation(args, _cache=None):
    """三检：存在性 / 效力 / 条文原文（给了引文再做逐字比对）。"""
    law_name = (args.get("law") or "").strip()
    article_in = args.get("article")
    citation_text = args.get("citation_text") or args.get("text")
    if not law_name:
        raise SourceError("law（法名）不能为空")

    checks = {}
    law, match = find_law(law_name)
    if law is None:
        checks["existence"] = {"ok": False,
                               "detail": f"国家法律法规数据库中未检索到《{law_name}》"}
        return {
            "schema": RESULT_SCHEMA, "tool": "verify_citation",
            "input": {"law": law_name, "article": article_in},
            "law": None, "checks": checks, "verdict": "not_verified",
            "verdict_zh": "未通过核验",
            "notes": ["法规本身不存在或名称有误，后续检查无从谈起；"
                      "请核对法名后重试，或改用 flk_search 先检索。"],
            "data_snapshot": _snapshot("live"),
        }
    checks["existence"] = {
        "ok": True,
        "detail": f"已在官方库定位到《{law['title']}》（匹配方式：{match}）",
    }
    status_ok = law["status_code"] == 3
    status_warn = law["status_code"] == 2
    checks["status"] = {
        "ok": status_ok,
        "warning": status_warn,
        "status": law["status"],
        "detail": (
            "该法规现行有效" if status_ok else
            "该法规已被修改：官方库现行文本为修改后版本，引用时须以现行文本为准" if status_warn else
            f"该法规当前效力状态为「{law['status']}」，不得作为现行有效依据引用"),
    }

    notes = []
    article_result = None
    if article_in is not None and str(article_in).strip() != "":
        num, sub = normalize_article(article_in)
        heading = article_heading(num, sub)
        article_check = {"article": heading}
        official_text, text_source, fetch_notes = fetch_official_article(
            law, num, sub, cache=_cache)
        notes.extend(fetch_notes)
        if text_source == "official_context_window":
            notes.append("逐字比对基于官方检索的上下文窗口中截取的条文段（非全文），"
                         "长条文可能被截断，结论等级相应降低")
        if official_text is None:
            article_check.update({
                "ok": False,
                "detail": f"在《{law['title']}》现行文本中未找到{heading}"
                          "（条号可能不存在、已漂移，或该条已在修订中调整）",
            })
        else:
            article_check.update({
                "ok": True,
                "official_text": official_text,
                "text_source": text_source,
                "detail": f"已取回{heading}官方原文",
            })
            if citation_text:
                given = _normalize_for_compare(citation_text)
                official = _normalize_for_compare(official_text)
                exact = given == official or (
                    text_source == "official_context_window"
                    and (given in official or official in given))
                text_check = {"ok": exact, "exact_match": exact,
                              "compared_against": text_source}
                if not exact:
                    ratio = difflib.SequenceMatcher(None, given, official).ratio()
                    text_check["similarity"] = round(ratio, 4)
                    for i, (a, b) in enumerate(zip(given, official)):
                        if a != b:
                            text_check["first_diff_at"] = i
                            text_check["given_snippet"] = given[max(0, i - 10):i + 20]
                            text_check["official_snippet"] = official[max(0, i - 10):i + 20]
                            break
                    else:
                        text_check["first_diff_at"] = min(len(given), len(official))
                    text_check["detail"] = "引文与官方原文不逐字一致，差异见 first_diff_at 附近"
                else:
                    text_check["detail"] = "引文与官方原文逐字一致"
                checks["text"] = text_check
        checks["article"] = article_check
        article_result = article_check

    # 总判定（三态）：全过=verified；存在但有警告/降级=exists_only；硬伤=not_verified
    hard_fail = (not checks["existence"]["ok"]
                 or law["status_code"] in (1, 4)
                 or (article_result is not None and not article_result.get("ok"))
                 or (checks.get("text") is not None and not checks["text"]["ok"]))
    degraded = (status_warn
                or (article_result is not None
                    and article_result.get("text_source") == "official_context_window"))
    if hard_fail:
        verdict, verdict_zh = "not_verified", "未通过核验"
    elif degraded:
        verdict, verdict_zh = "exists_only", "仅确认存在（效力或比对存在保留，见 checks）"
    else:
        verdict, verdict_zh = "verified", "已核验（存在性＋原文＋效力）"
    return {
        "schema": RESULT_SCHEMA, "tool": "verify_citation",
        "input": {"law": law_name, "article": article_in,
                  "citation_text_provided": bool(citation_text)},
        "law": law, "checks": checks,
        "verdict": verdict, "verdict_zh": verdict_zh,
        "notes": notes,
        "data_snapshot": _snapshot("live"),
    }


# ---------------------------------------------------------------------------
# 快照库（SQLite，条文级）：tools/build_snapshot.py 离线构建，server 只读
# ---------------------------------------------------------------------------

def snapshot_db_path():
    env = os.environ.get("CN_LAW_VERIFY_SNAPSHOT_DB")
    if env:
        return env
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "data", "snapshot.db")


def _open_snapshot():
    path = snapshot_db_path()
    if not os.path.exists(path):
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def snapshot_info():
    """快照库概况；无库时返回 None。"""
    conn = _open_snapshot()
    if conn is None:
        return None
    try:
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM meta")}
        law_count = conn.execute("SELECT COUNT(*) FROM laws").fetchone()[0]
        art_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        return {"path": snapshot_db_path(),
                "built_at": meta.get("built_at"),
                "law_count": law_count, "article_count": art_count,
                "coverage": json.loads(meta.get("coverage") or "[]")}
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def _snapshot_law_dict(row):
    status_zh, status_en, code = map_status(row["status_code"])
    return {
        "id": row["bbbs"], "title": row["title"],
        "authority": row["authority"] or "", "category": row["category"] or "",
        "promulgated": row["promulgated"] or "",
        "effective_from": row["effective_from"] or "",
        "status": status_zh, "status_en": status_en, "status_code": code,
        "source_url": row["source_url"],
        "article_count": row["article_count"],
    }


def snapshot_find_law(name):
    """在快照库中按法名定位（精确 → 简称包含，同名优先现行有效版）。"""
    conn = _open_snapshot()
    if conn is None:
        return None
    try:
        rows = conn.execute("SELECT * FROM laws").fetchall()
    finally:
        conn.close()
    name = (name or "").strip().strip("《》").strip()
    if not name:
        return None
    exact = [r for r in rows if r["title"] == name]
    pool = exact or [r for r in rows
                     if name in r["title"] or r["title"] in name]
    if not pool:
        return None
    pool.sort(key=lambda r: (_STATUS_PREFERENCE.get(r["status_code"], 9),
                             abs(len(r["title"]) - len(name))))
    return _snapshot_law_dict(pool[0])


def snapshot_get_article(bbbs, num, sub=None):
    """按 (法规, 条号, 之几) 取快照条文；sub 约定：无「之几」存 0。"""
    conn = _open_snapshot()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT * FROM articles WHERE bbbs=? AND num=? AND sub=?",
            (bbbs, num, sub or 0)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def snapshot_search(terms, limit=10):
    """快照库条文检索：同一条文须同时含全部关键词（AND），按命中次数排序。"""
    conn = _open_snapshot()
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT a.bbbs, a.num, a.sub, a.heading, a.text, "
            "l.title, l.status_code FROM articles a "
            "JOIN laws l ON l.bbbs = a.bbbs").fetchall()
    finally:
        conn.close()
    hits = []
    for r in rows:
        text = r["text"]
        if all(t in text for t in terms):
            score = sum(text.count(t) for t in terms)
            pos = min(text.find(t) for t in terms)
            hits.append((score, r, pos))
    hits.sort(key=lambda h: -h[0])
    out = []
    for score, r, pos in hits[:limit]:
        text = r["text"]
        start = max(0, pos - 60)
        end = min(len(text), pos + 140)
        snippet = ("…" if start > 0 else "") + text[start:end] + \
                  ("…" if end < len(text) else "")
        status_zh, status_en, code = map_status(r["status_code"])
        out.append({
            "law_title": r["title"], "law_id": r["bbbs"],
            "article": r["heading"], "article_num": r["num"],
            "article_sub": r["sub"] or None, "snippet": snippet,
            "status": status_zh, "status_en": status_en,
            "score": score,
            "source_url": f"https://flk.npc.gov.cn/detail.html?bbbs={r['bbbs']}",
        })
    return out


def _snapshot_stamp(channel, as_of=None):
    stamp = {
        "channel": channel,
        "source": "国家法律法规数据库 flk.npc.gov.cn",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if as_of:
        stamp["snapshot_built_at"] = as_of
        stamp["note"] = ("结论基于本地快照（构建时间见 snapshot_built_at），"
                          "非实时数据；最新效力请以官方源实时复核为准")
    return stamp


# ---------------------------------------------------------------------------
# get_law：元数据＋章节目录＋版本沿革（实时通道）
# ---------------------------------------------------------------------------

def tool_get_law(args):
    law = None
    bbbs = (args.get("id") or "").strip()
    if bbbs:
        law = {"id": bbbs}
    else:
        name = (args.get("law") or args.get("query") or "").strip()
        if not name:
            raise SourceError("get_law 需要 id（bbbs）或 law（法名）")
        found, _match = find_law(name)
        if found is None:
            raise SourceError(f"国家法律法规数据库中未检索到《{name}》")
        law = found
    details = fetch_law_details(law["id"])
    status_zh, status_en, code = map_status(details.get("sxx"))
    flat = flatten_toc(details.get("content") or {})
    article_count = toc_article_count(flat)
    count_note = None
    if article_count == 0:
        # 个别法规的官方目录树只到章/节层级、不含条级节点；
        # 快照收录了该法时以快照切条数为准并注明，否则如实置空。
        snap_law = snapshot_find_law(clean_title(details.get("title")))
        if snap_law is not None:
            article_count = snap_law["article_count"]
            count_note = "官方目录树未细化到条，条数取自本地快照切条计数"
        else:
            article_count = None
            count_note = "官方目录树未细化到条，条数未能从目录统计"
    # 目录只保留到章/节层级＋条数统计，逐条目录太长不整表返回
    outline = [item for item in flat
               if not _ARTICLE_TITLE_ONLY_RE.match(item["title"])]
    versions = []
    for v in details.get("lsyg") or []:
        if isinstance(v, dict):
            versions.append({
                "id": v.get("bbbs"), "title": clean_title(v.get("title")),
                "promulgated": v.get("gbrq") or "",
                "is_current": bool(v.get("highLight")),
                "source_url": "https://flk.npc.gov.cn/detail.html"
                              f"?bbbs={v.get('bbbs')}",
            })
    versions.sort(key=lambda v: v["promulgated"] or "", reverse=True)
    related = []
    for item in details.get("xgzl") or []:
        if isinstance(item, dict) and item.get("title"):
            related.append({"title": clean_title(item.get("title")),
                            "type": item.get("busiType") or ""})
    return {
        "schema": RESULT_SCHEMA, "tool": "get_law",
        "law": {
            "id": law["id"], "title": clean_title(details.get("title")),
            "authority": details.get("zdjgName") or "",
            "category": details.get("flxz") or "",
            "promulgated": details.get("gbrq") or "",
            "effective_from": details.get("sxrq") or "",
            "status": status_zh, "status_en": status_en, "status_code": code,
            "source_url": f"https://flk.npc.gov.cn/detail.html?bbbs={law['id']}",
        },
        "article_count": article_count,
        "article_count_note": count_note,
        "outline": outline,
        "versions": versions,
        "versions_note": ("版本沿革来自官方详情页 lsyg 字段；is_current 为"
                          "官方标注的当前版本" if versions else
                          "官方详情页未提供版本沿革（该法规在库中仅此一版）"),
        "related_materials": related,
        "data_snapshot": _snapshot_stamp("live"),
    }


# ---------------------------------------------------------------------------
# get_article：单条逐字原文（快照优先，实时兜底）
# ---------------------------------------------------------------------------

def tool_get_article(args):
    law_name = (args.get("law") or "").strip()
    bbbs_in = (args.get("id") or "").strip()
    article_in = args.get("article")
    if article_in is None or str(article_in).strip() == "":
        raise SourceError("article（条号）不能为空")
    num, sub = normalize_article(article_in)
    heading = article_heading(num, sub)
    notes = []

    snap_law = None
    if law_name:
        snap_law = snapshot_find_law(law_name)
    if bbbs_in and snap_law is None:
        info = snapshot_info()
        if info:
            for item in info["coverage"]:
                if item.get("id") == bbbs_in:
                    snap_law = snapshot_find_law(item["title"])
                    break
    if snap_law is not None:
        art = snapshot_get_article(snap_law["id"], num, sub)
        if art is not None:
            info = snapshot_info()
            return {
                "schema": RESULT_SCHEMA, "tool": "get_article",
                "law": snap_law, "article": heading,
                "text": art["text"], "text_source": "local_snapshot",
                "notes": ["原文取自本地快照库（官方 DOCX 全文切条），"
                          "效力状态以快照构建时官方标注为准"],
                "data_snapshot": _snapshot_stamp(
                    "snapshot", as_of=info["built_at"] if info else None),
            }
        notes.append(f"快照库收录了《{snap_law['title']}》，但其中未找到{heading}，"
                     "转实时通道复核（条号可能不存在，也可能快照未覆盖）")

    # 实时通道
    if bbbs_in and not law_name:
        details = fetch_law_details(bbbs_in)
        law = {"id": bbbs_in, "title": clean_title(details.get("title")),
               "authority": details.get("zdjgName") or "",
               "category": details.get("flxz") or "",
               "promulgated": details.get("gbrq") or "",
               "effective_from": details.get("sxrq") or "",
               **dict(zip(("status", "status_en", "status_code"),
                          map_status(details.get("sxx")))),
               "source_url": f"https://flk.npc.gov.cn/detail.html?bbbs={bbbs_in}"}
    else:
        law, _match = find_law(law_name)
        if law is None:
            raise SourceError(f"国家法律法规数据库中未检索到《{law_name}》")
    text, text_source, fetch_notes = fetch_official_article(law, num, sub)
    notes.extend(fetch_notes)
    if text is None:
        raise SourceError(f"在《{law['title']}》现行文本中未找到{heading}"
                          "（条号可能不存在或已漂移）")
    return {
        "schema": RESULT_SCHEMA, "tool": "get_article",
        "law": law, "article": heading, "text": text,
        "text_source": text_source, "notes": notes,
        "data_snapshot": _snapshot_stamp("live"),
    }


# ---------------------------------------------------------------------------
# search_articles：跨法规条文级检索（快照为主，实时兜底并注明覆盖）
# ---------------------------------------------------------------------------

_MAX_LIVE_CANDIDATES = 5


def _split_terms(keywords):
    terms = [t for t in re.split(r"[\s,，、;；]+", (keywords or "").strip()) if t]
    if not terms:
        raise SourceError("keywords 不能为空")
    return terms


def tool_search_articles(args):
    keywords = (args.get("keywords") or "").strip()
    terms = _split_terms(keywords)
    limit = max(1, min(int(args.get("limit", 10)), 30))
    notes = []

    snap = snapshot_search(terms, limit=limit)
    info = snapshot_info()
    if snap:
        return {
            "schema": RESULT_SCHEMA, "tool": "search_articles",
            "keywords": terms, "total_in_snapshot": len(snap),
            "results": snap, "notes": notes,
            "coverage": (f"检索范围＝本地快照库（{info['law_count']} 部法规、"
                         f"{info['article_count']} 条，构建于 {info['built_at']}）；"
                         "快照之外的法规不在本次检索范围内"),
            "data_snapshot": _snapshot_stamp(
                "snapshot", as_of=info["built_at"] if info else None),
        }
    if info:
        notes.append("快照库内未命中，转实时通道兜底检索")

    # 实时兜底：官方正文检索找候选法规 → 逐部取全文切条命中。
    # 用精确检索：官方正文模糊检索几乎不做相关性排序（实测长句查询
    # 返回 3 万余条乱序结果），精确短语检索才可定位候选法规；
    # 整组关键词无候选时退到最长的一个关键词再试。
    _total, rows = flk_search_rows(" ".join(terms), search_type=1,
                                   search_range=2, page_size=_MAX_LIVE_CANDIDATES)
    if not rows and len(terms) > 1:
        longest = max(terms, key=len)
        _total, rows = flk_search_rows(longest, search_type=1,
                                       search_range=2,
                                       page_size=_MAX_LIVE_CANDIDATES)
        if rows:
            notes.append(f"整组关键词无精确候选，改用「{longest}」单关键词"
                         "定位候选法规，条文命中仍按全部关键词过滤")
    candidates = [row_to_result(r) for r in rows][:_MAX_LIVE_CANDIDATES]
    results = []
    for law in candidates:
        try:
            paragraphs = fetch_law_docx_paragraphs(law["id"])
        except SourceError as e:
            notes.append(f"《{law['title']}》全文获取失败，跳过：{e}")
            continue
        for art in split_articles(paragraphs):
            if all(t in art["text"] for t in terms):
                pos = min(art["text"].find(t) for t in terms)
                text = art["text"]
                start = max(0, pos - 60)
                end = min(len(text), pos + 140)
                results.append({
                    "law_title": law["title"], "law_id": law["id"],
                    "article": art["heading"], "article_num": art["num"],
                    "article_sub": art["sub"],
                    "snippet": ("…" if start > 0 else "") + text[start:end]
                               + ("…" if end < len(text) else ""),
                    "status": law["status"], "status_en": law["status_en"],
                    "score": sum(text.count(t) for t in terms),
                    "source_url": law["source_url"],
                })
        if len(results) >= limit:
            break
    results.sort(key=lambda r: -r["score"])
    return {
        "schema": RESULT_SCHEMA, "tool": "search_articles",
        "keywords": terms, "total_in_candidates": len(results),
        "results": results[:limit], "notes": notes,
        "coverage": (f"实时通道兜底：仅检索了官方正文精确检索返回的前 "
                     f"{len(candidates)} 部候选法规，不是全库检索；"
                     "未命中不等于不存在含该关键词的条文"),
        "data_snapshot": _snapshot_stamp("live"),
    }


# ---------------------------------------------------------------------------
# check_citations_in_text：整段文本引注抽取＋逐条三检
# ---------------------------------------------------------------------------

_MAX_CITATIONS_PER_RUN = 20


def tool_check_citations_in_text(args):
    text = args.get("text") or ""
    if not text.strip():
        raise SourceError("text 不能为空")
    found = extract_citations(text)
    seen, unique = set(), []
    for c in found:
        key = (c["law"], c["article_num"], c["article_sub"])
        if key not in seen:
            seen.add(key)
            unique.append(c)
    truncated = len(unique) > _MAX_CITATIONS_PER_RUN
    to_check = unique[:_MAX_CITATIONS_PER_RUN]
    cache = {}
    results = []
    for c in to_check:
        entry = {"law": c["law"], "article": c["article"],
                 "span": c["span"]}
        try:
            out = tool_verify_citation(
                {"law": c["law"], "article": c["article"]}, _cache=cache)
            entry["verdict"] = out["verdict"]
            entry["verdict_zh"] = out["verdict_zh"]
            entry["law_matched"] = out["law"]["title"] if out.get("law") else None
            entry["status"] = out["law"]["status"] if out.get("law") else None
            entry["source_url"] = out["law"]["source_url"] if out.get("law") else None
            problems = []
            for key, chk in (out.get("checks") or {}).items():
                if isinstance(chk, dict) and chk.get("ok") is False:
                    problems.append(f"{key}：{chk.get('detail', '')}")
            entry["problems"] = problems
            art_chk = (out.get("checks") or {}).get("article") or {}
            if art_chk.get("official_text"):
                entry["official_text"] = art_chk["official_text"]
                entry["text_source"] = art_chk.get("text_source")
        except SourceError as e:
            entry["verdict"] = "error"
            entry["verdict_zh"] = "核验未能完成"
            entry["problems"] = [str(e)]
        results.append(entry)
    counts = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    notes = []
    if truncated:
        notes.append(f"文中引注去重后共 {len(unique)} 条，单次上限 "
                     f"{_MAX_CITATIONS_PER_RUN} 条，其余未检，请分段提交")
    if len(found) != len(unique):
        notes.append(f"原文共出现 {len(found)} 处引注，去重后 {len(unique)} 条")
    return {
        "schema": RESULT_SCHEMA, "tool": "check_citations_in_text",
        "total_found": len(found), "unique": len(unique),
        "checked": len(results), "summary": counts,
        "results": results,
        "problems": [r for r in results if r["verdict"] != "verified"],
        "notes": notes,
        "data_snapshot": _snapshot_stamp("live"),
    }


# ---------------------------------------------------------------------------
# lookup：一句话直达（机器结论／人工复核清单 两段式）
# ---------------------------------------------------------------------------

_LAW_ALIASES = {
    "民法典": "中华人民共和国民法典",
    "刑法": "中华人民共和国刑法",
    "民诉法": "中华人民共和国民事诉讼法",
    "民诉": "中华人民共和国民事诉讼法",
    "刑诉法": "中华人民共和国刑事诉讼法",
    "刑诉": "中华人民共和国刑事诉讼法",
    "行诉法": "中华人民共和国行政诉讼法",
    "公司法": "中华人民共和国公司法",
    "劳动法": "中华人民共和国劳动法",
    "劳动合同法": "中华人民共和国劳动合同法",
    "消法": "中华人民共和国消费者权益保护法",
    "消费者权益保护法": "中华人民共和国消费者权益保护法",
    "个保法": "中华人民共和国个人信息保护法",
    "治安管理处罚法": "中华人民共和国治安管理处罚法",
    "道路交通安全法": "中华人民共和国道路交通安全法",
    "道交法": "中华人民共和国道路交通安全法",
    "著作权法": "中华人民共和国著作权法",
    "商标法": "中华人民共和国商标法",
    "专利法": "中华人民共和国专利法",
    "食品安全法": "中华人民共和国食品安全法",
    "未成年人保护法": "中华人民共和国未成年人保护法",
    "宪法": "中华人民共和国宪法",
}

_LOOKUP_ARTICLE_RE = re.compile(
    r"第?\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")
_LOOKUP_FILLER_RE = re.compile(
    r"(帮我|请)?(查一?下|查询|看看|看一下|搜索|检索)?|是什么|怎么规定|"
    r"规定的?|原文|内容|全文|讲的是什么|说的是什么|的|了|吗|呢|[？?。，、\s]")


def parse_lookup_query(query):
    """解析一句话查询 → (法名, 条号num|None, sub|None, 解析方式)。

    支持：《民法典》第五百七十七条 / 民法典577条 / 劳动合同法第三十九条
    是什么 / 民诉法第64条。解析不了条号时只回法名（走法规级查询）。
    """
    q = (query or "").strip()
    if not q:
        raise SourceError("query 不能为空")
    m = re.search(r"《([^》]{2,60})》", q)
    if m:
        law_name = m.group(1).strip()
        rest = q[:m.start()] + q[m.end():]
        am = _LOOKUP_ARTICLE_RE.search(rest)
        if am:
            return law_name, cn_to_int(am.group(1)), \
                cn_to_int(am.group(2)) if am.group(2) else None, "bracket"
        return law_name, None, None, "bracket"
    am = _LOOKUP_ARTICLE_RE.search(q)
    if am:
        law_part = q[:am.start()]
        law_name = _LOOKUP_FILLER_RE.sub("", law_part).strip("《》 ")
        if law_name:
            return law_name, cn_to_int(am.group(1)), \
                cn_to_int(am.group(2)) if am.group(2) else None, "inline"
    law_name = _LOOKUP_FILLER_RE.sub("", q).strip("《》 ")
    if not law_name:
        raise SourceError(f"无法从「{query}」中解析出法名")
    return law_name, None, None, "law_only"


_HUMAN_REVIEW_BASE = [
    "效力时效性：结论基于工具标注的通道与取数时间，个案适用前建议再核对一次官方源",
    "论点支持关系不在本工具核验范围：条文真实、现行，不等于它支持你的具体论点（可用 check_support 做启发式初筛，最终须人工判断）",
    "具体适用须结合案件事实、地方规定与司法解释，本结果不构成法律意见",
]


def tool_lookup(args):
    query = (args.get("query") or "").strip()
    law_name, num, sub, via = parse_lookup_query(query)
    alias_hit = _LAW_ALIASES.get(law_name)
    lookup_name = alias_hit or law_name
    notes = []
    if alias_hit:
        notes.append(f"「{law_name}」按常用简称映射为《{alias_hit}》")
    if num is not None:
        heading = article_heading(num, sub)
        out = tool_get_article({"law": lookup_name, "article": heading})
        law = out["law"]
        conclusion = {
            "kind": "article",
            "law": law["title"], "article": heading,
            "status": law["status"], "text": out["text"],
            "text_source": out["text_source"],
            "source_url": law["source_url"],
            "summary": (f"《{law['title']}》{heading}已取回官方原文，"
                        f"效力状态「{law['status']}」"
                        f"（通道：{out['data_snapshot']['channel']}）"),
        }
        checklist = list(_HUMAN_REVIEW_BASE)
        if law["status_code"] != 3:
            checklist.insert(0, f"该法规效力状态为「{law['status']}」，"
                                "不得作为现行有效依据直接引用")
        if out["text_source"] == "official_context_window":
            checklist.insert(0, "本次原文取自上下文窗口（非全文），"
                                "长条文可能被截断，引用前请开官方链接核对全文")
        return {
            "schema": RESULT_SCHEMA, "tool": "lookup",
            "input": {"query": query, "parsed_law": lookup_name,
                      "parsed_article": heading, "parse_via": via},
            "machine_conclusion": conclusion,
            "human_review_checklist": checklist,
            "notes": notes + (out.get("notes") or []),
            "data_snapshot": out["data_snapshot"],
        }
    out = tool_get_law({"law": lookup_name})
    law = out["law"]
    conclusion = {
        "kind": "law",
        "law": law["title"], "status": law["status"],
        "authority": law["authority"], "category": law["category"],
        "promulgated": law["promulgated"],
        "effective_from": law["effective_from"],
        "article_count": out["article_count"],
        "versions": out["versions"],
        "source_url": law["source_url"],
        "summary": (f"《{law['title']}》：{law['authority']}制定，"
                    f"{law['promulgated']}公布、{law['effective_from']}施行，"
                    + (f"共 {out['article_count']} 条，"
                       if out["article_count"] else "条数见目录，")
                    + f"效力状态「{law['status']}」"),
    }
    return {
        "schema": RESULT_SCHEMA, "tool": "lookup",
        "input": {"query": query, "parsed_law": lookup_name,
                  "parsed_article": None, "parse_via": via},
        "machine_conclusion": conclusion,
        "human_review_checklist": list(_HUMAN_REVIEW_BASE),
        "notes": notes,
        "data_snapshot": out["data_snapshot"],
    }


# ---------------------------------------------------------------------------
# check_support：蕴含初筛（零依赖启发式，明示不可替代人工判断）
# ---------------------------------------------------------------------------

_SUPPORT_DISCLAIMER = ("本结论由零依赖词面启发式生成（关键词覆盖、数字一致性、"
                       "否定极性冲突三类信号），只是初筛，不是语义判定，"
                       "不能替代人工判断；引注是否真正支持论断，"
                       "请结合条文全文与案件事实人工复核。")

_NEG_WORDS = ("不得", "不能", "不应", "禁止", "不准", "无需", "无须",
              "不承担", "不予", "不会", "没有", "不发生",
              "不产生", "不适用", "不影响", "不限制")
_POS_WORDS = ("应当", "必须", "有权", "可以", "承担", "赔偿", "履行",
              "有权要求", "应当承担")
# 极性冲突用定向模式：裸字「不」会把引文里描述违约情形的「不履行」
# 误判成否定表述，故只认「免责表述 × 义务表述」「禁止表述 × 许可表述」
# 这类贴在义务谓语上的组合。
_EXEMPTION_RE = re.compile(r"(不承担|无需|无须|不必|免除|可以不|有权拒绝|拒绝承担)")
_DUTY_RE = re.compile(r"(应当承担|必须承担|承担[^，。；]{0,6}责任|应当赔偿|应当履行)")
_PROHIBIT_RE = re.compile(r"(不得|禁止|不准)")
_PERMIT_RE = re.compile(r"(可以|有权)")
_NUMBER_RE = re.compile(
    r"([0-9]+(?:\.[0-9]+)?|[零一二三四五六七八九十百千]+)"
    r"\s*(%|％|元|万元|亿元|日|天|个月|月|年|倍|条|款|项|人|次|级)?")
_STOP_BIGRAMS = {"的规", "规定", "应当", "可以", "对于", "以及", "或者",
                "但是", "如果", "进行", "相关", "根据", "按照", "依法",
                "本法", "其中", "以上", "以下", "以内", "之日起"}


def _extract_numbers(text):
    out = []
    for m in _NUMBER_RE.finditer(text or ""):
        raw, unit = m.group(1), m.group(2) or ""
        try:
            value = float(raw) if raw.replace(".", "").isdigit() \
                else float(cn_to_int(raw))
        except (ValueError, TypeError):
            continue
        out.append((value, unit))
    return out


def _content_bigrams(text):
    cleaned = re.sub(r"[^一-鿿A-Za-z0-9]", "", text or "")
    grams = set()
    for i in range(len(cleaned) - 1):
        g = cleaned[i:i + 2]
        if g not in _STOP_BIGRAMS and not g.isdigit():
            grams.add(g)
    return grams


def _has_negation(text):
    return [w for w in _NEG_WORDS if w in (text or "")]


def tool_check_support(args):
    claim = (args.get("claim") or "").strip()
    if not claim:
        raise SourceError("claim（待判断的论断）不能为空")
    citation_text = (args.get("citation_text") or "").strip()
    citation_meta = {}
    if not citation_text:
        law_name = (args.get("law") or "").strip()
        article_in = args.get("article")
        if not law_name or article_in is None:
            raise SourceError("需要 citation_text，或 law＋article 指定引注来源")
        out = tool_get_article({"law": law_name, "article": article_in})
        citation_text = out["text"]
        citation_meta = {"law": out["law"]["title"],
                         "article": out["article"],
                         "status": out["law"]["status"],
                         "source_url": out["law"]["source_url"],
                         "text_source": out["text_source"]}
    signals = []
    conflicts = []

    # 信号 1：数字一致性（论断里的具体数字必须在引文中出现）
    claim_nums = _extract_numbers(claim)
    cite_nums = _extract_numbers(citation_text)
    cite_num_set = {(v, u) for v, u in cite_nums}
    cite_values = {v for v, _u in cite_nums}
    missing_nums = []
    for value, unit in claim_nums:
        if (value, unit) in cite_num_set:
            continue
        if unit and value in cite_values:
            signals.append(f"数字 {value} 在引文中出现但单位不同"
                           f"（论断用「{unit}」），请人工核对")
            continue
        if not unit and value in cite_values:
            continue
        missing_nums.append((value, unit))
    if missing_nums:
        desc = "、".join(f"{int(v) if v == int(v) else v}{u}"
                         for v, u in missing_nums)
        conflicts.append(f"论断中的数字 {desc} 未在引文中出现，"
                         "数字依据可能不存在或张冠李戴")

    # 信号 2：否定极性冲突（论断与引文一否一肯且词面高度重合时才算）
    claim_bigrams = _content_bigrams(claim)
    cite_bigrams = _content_bigrams(citation_text)
    coverage = (len(claim_bigrams & cite_bigrams) / len(claim_bigrams)
                if claim_bigrams else 0.0)
    claim_neg = _has_negation(claim)
    cite_neg = _has_negation(citation_text)
    if _EXEMPTION_RE.search(claim) and _DUTY_RE.search(citation_text) \
            and coverage >= 0.2:
        conflicts.append("极性冲突：论断在免除/否定责任，引文恰恰规定"
                         "应当承担责任（义务方向相反）")
    elif _DUTY_RE.search(claim) and _EXEMPTION_RE.search(citation_text) \
            and coverage >= 0.2:
        conflicts.append("极性冲突：论断主张应当承担责任，引文却含免责表述"
                         "（义务方向相反）")
    elif _PROHIBIT_RE.search(citation_text) and _PERMIT_RE.search(claim) \
            and not claim_neg and coverage >= 0.25:
        conflicts.append("极性疑似冲突：引文含禁止表述（不得/禁止/不准），"
                         "论断却表述为可以/有权")
    elif _PROHIBIT_RE.search(claim) and _PERMIT_RE.search(citation_text) \
            and not cite_neg and coverage >= 0.25:
        conflicts.append("极性疑似冲突：论断表述为禁止，引文却含许可表述"
                         "（可以/有权）")
    signals.append(f"关键词（二元组）覆盖率 {coverage:.2f}"
                   f"（论断特征 {len(claim_bigrams)} 个，命中 "
                   f"{len(claim_bigrams & cite_bigrams)} 个）")

    # 信号 3：法名错位（论断点名的法规与引文来源不一致）
    law_in_claim = re.search(r"《([^》]{2,60})》", claim)
    if law_in_claim and citation_meta.get("law") \
            and law_in_claim.group(1) not in citation_meta["law"] \
            and citation_meta["law"] not in law_in_claim.group(1):
        conflicts.append(f"论断点名《{law_in_claim.group(1)}》，"
                         f"但引文取自《{citation_meta['law']}》")

    if conflicts:
        verdict = "unsupported"
        verdict_zh = "初筛：不支持（检出硬冲突信号，见 reasons）"
    elif coverage >= 0.5:
        verdict = "supports"
        verdict_zh = "初筛：支持（词面高度重合且无冲突信号）"
    elif coverage >= 0.28:
        verdict = "partial"
        verdict_zh = "初筛：部分支持（词面部分重合，可能只覆盖论断的一部分）"
    else:
        verdict = "cannot_determine"
        verdict_zh = "初筛：无法判断（词面重合过低，启发式无从判定）"
    return {
        "schema": RESULT_SCHEMA, "tool": "check_support",
        "method": "lexical_heuristic_v1",
        "claim": claim, "citation": citation_meta or {"text_provided": True},
        "citation_text": citation_text,
        "verdict": verdict, "verdict_zh": verdict_zh,
        "reasons": conflicts + signals,
        "coverage": round(coverage, 4),
        "disclaimer": _SUPPORT_DISCLAIMER,
        "data_snapshot": _snapshot_stamp("local_heuristic"),
    }


TOOLS = [
    {
        "name": "flk_search",
        "description": "在国家法律法规数据库（flk.npc.gov.cn）官方源实时检索法规，"
                       "返回带效力状态与官方链接的法规列表。引用任何法条前先用它定位法规。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索词（法名或关键词）"},
                "mode": {"type": "string", "enum": ["exact", "fuzzy"],
                         "description": "exact=精确短语，fuzzy=模糊（默认）"},
                "scope": {"type": "string", "enum": ["title", "body"],
                          "description": "title=只查标题（默认），body=查正文"},
                "limit": {"type": "integer", "description": "返回条数，1–20，默认 10"},
            },
            "required": ["query"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_flk_search,
    },
    {
        "name": "verify_citation",
        "description": "核验一条法条引注：给法名＋条号（＋可选引文文本），回存在性、"
                       "效力状态、官方原文三检结论与总判定。法名不存在、条号不存在、"
                       "法规已废止/尚未生效、引文与原文不逐字一致都会被明确指出。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law": {"type": "string", "description": "法规名称，如「中华人民共和国民法典」"},
                "article": {"type": "string",
                            "description": "条号，如「第五百七十七条」「第577条」「577」"},
                "citation_text": {"type": "string",
                                  "description": "可选：待核验的引文文本，给了才做逐字比对"},
            },
            "required": ["law"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_verify_citation,
    },
    {
        "name": "get_law",
        "description": "取单部法规的元数据＋章节目录大纲＋版本沿革（实时通道，"
                       "数据来自官方详情页）：制定机关、公布/施行日期、效力状态、"
                       "条数、历次版本列表。给法名或 flk 文档 id（bbbs）均可。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law": {"type": "string", "description": "法规名称，如「中华人民共和国公司法」"},
                "id": {"type": "string", "description": "或：flk 文档 id（bbbs）"},
            },
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_get_law,
    },
    {
        "name": "get_article",
        "description": "取单条条文逐字原文：本地快照库优先（注明快照时间），"
                       "快照未收录时走官方实时通道（DOCX 全文整条截取，"
                       "降级时为上下文窗口并如实标注）。返回原文、效力状态与官方链接。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "law": {"type": "string", "description": "法规名称"},
                "id": {"type": "string", "description": "或：flk 文档 id（bbbs）"},
                "article": {"type": "string",
                            "description": "条号，如「第五百七十七条」「577」"},
            },
            "required": ["article"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_get_article,
    },
    {
        "name": "search_articles",
        "description": "跨法规的条文级全文检索：给一组关键词（空格分隔，须同时命中），"
                       "在本地快照库的全部条文中检索并注明覆盖范围；快照未命中时"
                       "走官方正文检索兜底（仅前 5 部候选法规，出参明确注明非全库检索）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keywords": {"type": "string",
                             "description": "关键词，多个以空格分隔，如「竞业限制 经济补偿」"},
                "limit": {"type": "integer", "description": "返回条数，1–30，默认 10"},
            },
            "required": ["keywords"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_search_articles,
    },
    {
        "name": "check_citations_in_text",
        "description": "整段文本引注核验：抽取文中全部《法名》第N条形式的引注，"
                       "逐条走存在性／效力／条文存在三检，返回逐条结论、汇总计数与"
                       "问题清单。单次上限 20 条（去重后），超出会注明未检条数。"
                       "注意：本工具核验引注本身真伪，不判断引注是否支持文中论点"
                       "（那是 check_support 的初筛范围）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "待核验的整段文本"},
            },
            "required": ["text"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_check_citations_in_text,
    },
    {
        "name": "lookup",
        "description": "一句话自然语言直达：如「民法典577条」「劳动合同法第三十九条"
                       "是什么」「查一下公司法」——自动解析法名＋条号（含常用简称"
                       "映射），取回原文或法规概览，输出两段式结果：机器结论"
                       "（已核验的事实）与人工复核清单（机器保证不了、须人工确认的项）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "一句话查询"},
            },
            "required": ["query"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_lookup,
    },
    {
        "name": "check_support",
        "description": "蕴含初筛（启发式，非语义判定）：判断一条引注是否大体支持"
                       "某个论断。方法为零依赖词面启发式（关键词覆盖、数字一致性、"
                       "否定极性冲突），输出 supports/partial/unsupported/"
                       "cannot_determine 四档＋逐条理由。重要：这是初筛，"
                       "不能替代人工判断；每次输出都带此口径说明。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "claim": {"type": "string", "description": "待判断的论断"},
                "citation_text": {"type": "string",
                                  "description": "引注原文（与 law＋article 二选一）"},
                "law": {"type": "string", "description": "或：引注所在法规名称"},
                "article": {"type": "string", "description": "或：引注条号"},
            },
            "required": ["claim"],
        },
        "annotations": {"readOnlyHint": True},
        "_impl": tool_check_support,
    },
]

_TOOL_MAP = {t["name"]: t for t in TOOLS}


def public_tool_list():
    return [{k: v for k, v in t.items() if not k.startswith("_")} for t in TOOLS]


# ---------------------------------------------------------------------------
# MCP stdio JSON-RPC 最小闭环
# ---------------------------------------------------------------------------

def handle_request(msg):
    method = msg.get("method")
    mid = msg.get("id")
    params = msg.get("params") or {}
    if method == "initialize":
        requested = params.get("protocolVersion")
        version = requested if requested in SUPPORTED_PROTOCOLS else DEFAULT_PROTOCOL
        return {"protocolVersion": version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}}
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": public_tool_list()}
    if method == "tools/call":
        name = params.get("name")
        tool = _TOOL_MAP.get(name)
        if tool is None:
            return {"content": [{"type": "text",
                                 "text": f"未知工具：{name}"}],
                    "isError": True}
        try:
            result = tool["_impl"](params.get("arguments") or {})
            return {"content": [{"type": "text",
                                 "text": json.dumps(result, ensure_ascii=False,
                                                    indent=2)}],
                    "isError": False}
        except (SourceError, ValueError) as e:
            return {"content": [{"type": "text", "text": f"错误：{e}"}],
                    "isError": True}
    if mid is None:
        return None  # 通知，无需响应
    return {"_error": {"code": -32601, "message": f"Method not found: {method}"}}


def serve_stdio():
    log(f"{SERVER_NAME} v{SERVER_VERSION} stdio 就绪")
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("id") is None and msg.get("method") != "initialize":
            handle_request(msg)  # 通知类消息：处理但不响应
            continue
        result = handle_request(msg)
        if result is None:
            continue
        if isinstance(result, dict) and "_error" in result:
            resp = {"jsonrpc": "2.0", "id": msg.get("id"),
                    "error": result["_error"]}
        else:
            resp = {"jsonrpc": "2.0", "id": msg.get("id"), "result": result}
        out.write(json.dumps(resp, ensure_ascii=False) + "\n")
        out.flush()


# ---------------------------------------------------------------------------
# Streamable HTTP 传输（与 stdio 共用 handle_request 同一核验内核）
# ---------------------------------------------------------------------------

def _jsonrpc_response(msg, result):
    if isinstance(result, dict) and "_error" in result:
        return {"jsonrpc": "2.0", "id": msg.get("id"), "error": result["_error"]}
    return {"jsonrpc": "2.0", "id": msg.get("id"), "result": result}


def serve_http(host, port):
    """MCP Streamable HTTP：POST /mcp 收 JSON-RPC（单条或批量），

    请求回 application/json，通知回 202；GET 不提供服务端推送，回 405。
    无状态实现：不要求 Mcp-Session-Id（与本 server 的无状态请求处理一致）。
    默认只绑 127.0.0.1；对外托管时请置于反向代理之后并自行加访问控制。
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # 走 stderr，避免污染
            log("http:", fmt % args)

        def _send(self, code, body=None, ctype="application/json"):
            payload = b"" if body is None else \
                json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if payload:
                self.wfile.write(payload)

        def do_GET(self):
            if self.path.rstrip("/") in ("/mcp", ""):
                self._send(405, {"error": "本 server 不提供 GET/SSE 通道，"
                                          "请用 POST /mcp"})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path.rstrip("/") not in ("/mcp", ""):
                self._send(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = 0
            raw = self.rfile.read(length) if length > 0 else b""
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self._send(400, {"jsonrpc": "2.0", "id": None,
                                 "error": {"code": -32700,
                                           "message": "Parse error"}})
                return
            messages = payload if isinstance(payload, list) else [payload]
            responses = []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                result = handle_request(msg)
                if msg.get("id") is not None and result is not None:
                    responses.append(_jsonrpc_response(msg, result))
            if not responses:
                self._send(202)
            elif isinstance(payload, list):
                self._send(200, responses)
            else:
                self._send(200, responses[0])

    server = ThreadingHTTPServer((host, port), Handler)
    log(f"{SERVER_NAME} v{SERVER_VERSION} Streamable HTTP 就绪："
        f"http://{host}:{port}/mcp")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def main():
    argv = sys.argv[1:]
    if "--version" in argv:
        print(f"{SERVER_NAME} {SERVER_VERSION}")
        return 0
    if "--http" in argv:
        host = os.environ.get("CN_LAW_VERIFY_HTTP_HOST", "127.0.0.1")
        port = int(os.environ.get("CN_LAW_VERIFY_HTTP_PORT", "8765"))
        for i, a in enumerate(argv):
            if a == "--host" and i + 1 < len(argv):
                host = argv[i + 1]
            if a == "--port" and i + 1 < len(argv):
                port = int(argv[i + 1])
        return serve_http(host, port)
    serve_stdio()
    return 0


if __name__ == "__main__":
    sys.exit(main())
