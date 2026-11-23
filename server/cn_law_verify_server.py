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

import base64
import difflib
import gzip
import http.client
import io
import json
import os
import re
import socket
import sqlite3
import ssl
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

SERVER_NAME = "cn-law-verify"
SERVER_VERSION = "0.5.0"
RESULT_SCHEMA = "cn-law-verify/result@1"
SUPPORTED_PROTOCOLS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
DEFAULT_PROTOCOL = "2025-03-26"

FLK_BASE = "https://flk.npc.gov.cn"
# API 请求只许发往这几个官方域名；下载跳转另许联通云对象存储域名后缀
# （flk 官方 DOCX 的签名下载地址所在域名），且逐跳复检。
# 857 三库分工补齐后新增：司法部行政法规库 xzfg.moj.gov.cn、
# 国家规章库搜索网关 sousuoht.www.gov.cn。
API_HOSTS = {"flk.npc.gov.cn", "wb.flk.npc.gov.cn",
             "xzfg.moj.gov.cn", "sousuoht.www.gov.cn"}
DOWNLOAD_HOST_SUFFIXES = (".cucloud.cn",)
USER_AGENT = "cn-law-verify/0.4.0 (+https://github.com/songoao25/cn-law-verify)"

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


def _describe_network_error(e):
    """把连接层异常翻成人话：只说发生了什么，不外漏原始异常字样。

    报错口径（第 857 项）：说清哪一步、什么问题、下一步怎么办——
    原始异常（如 URLError 的 repr）只进 stderr 日志，不进用户可见文本。
    """
    if isinstance(e, urllib.error.URLError):
        reason = getattr(e, "reason", None)
        if isinstance(reason, ssl.SSLError):
            return "与官方源的加密连接校验失败"
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return "连接国家法律法规数据库超时"
        return "无法连接国家法律法规数据库（网络不通或连接被中途中断）"
    if isinstance(e, http.client.RemoteDisconnected):
        return "与国家法律法规数据库的连接被对方中途断开"
    if isinstance(e, http.client.IncompleteRead):
        return "官方源的响应传输不完整（内容被截断）"
    if isinstance(e, (TimeoutError, socket.timeout)):
        return "连接国家法律法规数据库超时"
    if isinstance(e, ssl.SSLError):
        return "与官方源的加密连接校验失败"
    return "与国家法律法规数据库的连接出现异常"


_NEXT_STEP_ON_NETWORK_FAILURE = (
    "下一步：①稍后重试；②要核验的法规若在本地快照覆盖内，"
    "可用 get_article / search_articles 先查快照原文；"
    "③也可打开 https://flk.npc.gov.cn 人工核验")


def _network_failure_error(step, last_err):
    return SourceError(f"{step}没走通：{_describe_network_error(last_err)}。"
                       f"{_NEXT_STEP_ON_NETWORK_FAILURE}")


# ---------------------------------------------------------------------------
# 参数校验：类型不对只许返回人话工具错误，绝不许打崩进程
# ---------------------------------------------------------------------------

def _type_zh(v):
    if isinstance(v, bool):
        return "布尔值"
    if isinstance(v, str):
        return "文本"
    if isinstance(v, int):
        return "整数"
    if isinstance(v, float):
        return "小数"
    if isinstance(v, list):
        return "列表"
    if isinstance(v, dict):
        return "对象"
    if v is None:
        return "空值"
    return type(v).__name__


def _arg_str(args, key, *, required=False, hint=""):
    """取字符串参数；类型不对抛人话 SourceError（不让 AttributeError 外泄）。"""
    v = args.get(key)
    if v is None:
        if required:
            raise SourceError(f"缺少必填参数 {key}。{hint}".strip())
        return ""
    if not isinstance(v, str):
        raise SourceError(
            f"参数 {key} 类型不对：需要文本，收到的是{_type_zh(v)}。"
            f"下一步：请把 {key} 按说明写成文本再发。{hint}".strip())
    return v


def _arg_int(args, key, default, lo, hi):
    """取整数参数（limit 类）；数字字符串/整值小数可纠正，其余人话报错。"""
    v = args.get(key, default)
    if isinstance(v, bool):
        raise SourceError(f"参数 {key} 类型不对：需要整数，收到的是布尔值。"
                          f"下一步：请把 {key} 写成 {lo}–{hi} 之间的整数。")
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if isinstance(v, str) and re.fullmatch(r"[0-9]+", v.strip()):
        v = int(v.strip())
    if not isinstance(v, int):
        raise SourceError(
            f"参数 {key} 类型不对：需要整数，收到的是{_type_zh(v)}。"
            f"下一步：请把 {key} 写成 {lo}–{hi} 之间的整数。")
    return max(lo, min(v, hi))


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
    raise _network_failure_error("向官方源发请求这一步", last_err)


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

_NAME_EXT_KEYWORDS = ("实施条例", "实施细则", "条例", "细则", "规定",
                      "办法", "解释", "修正案", "规则", "意见", "决定")


def _name_mismatch_rest(name, title):
    """所给法名＝定位到的法规名＋实质后缀时，返回多出的部分；否则 None。

    859 新增（防御性）：包含匹配命中的若是「《民法典》被写成
    《民法典实施条例》」这类多后缀名称，定位到的其实是另一部法规，
    核验结论必须显式提示，不能默默按定位到的法规出结论。
    """
    if not title or name == title or title not in (name or ""):
        return None
    rest = name.replace(title, "", 1).strip("（）()·、，,：:；;【】[]《》 ")
    if rest and any(k in rest for k in _NAME_EXT_KEYWORDS):
        return rest
    return None


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
        chosen = contains[0]
        rest = _name_mismatch_rest(name, chosen["title"])
        if rest:
            chosen["name_mismatch"] = rest
        return chosen, "contains"
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
    raise _network_failure_error("解析官方下载地址这一步", last_err)


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
    raise SourceError(f"下载法规全文（DOCX）这一步没走通：多次断点续传后"
                      f"仍不完整（期望 {total} 字节，实得 {len(buf)} 字节；"
                      f"末次问题：{_describe_network_error(last_err)}）。"
                      f"{_NEXT_STEP_ON_NETWORK_FAILURE}")


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
    """中文数字 → 整数（支持到千位，够法条编号用）。

    859 加固：解析后做往返校验——结果按规范写法（int_to_cn）重渲染，
    与输入不一致即判非法。否则「第五七七条」这类无单位连写会被逐字
    覆盖、静默解析成 7，核验会打到完全不同的条文上还不自知。
    「两」按「二」的异体接受，「一十」按「十」的口语变体接受。
    """
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
    total += current
    canonical = int_to_cn(total)
    norm_in = s.replace("两", "二")
    if norm_in not in (canonical, "一" + canonical):
        raise ValueError(f"无法解析的中文数字：{s}")
    return total


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


_FW_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def _fw_norm(s):
    """全角数字 → 半角（逐字 1:1 替换，不改变字符串长度与位置）。"""
    return (s or "").translate(_FW_DIGITS)


_ARTICLE_INPUT_RE = re.compile(
    r"^第?\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?$")
_ARTICLE_SUFFIX_RE = re.compile(
    r"(?:\s*第\s*(?:[0-9]+|[零一二三四五六七八九十百千两]+)\s*[款项]"
    r"(?:\s*第\s*(?:[0-9]+|[零一二三四五六七八九十百千两]+)\s*目)?)+\s*$")


def normalize_article(article):
    """条号入参归一化 → (条数 int, 之几 int|None)。

    接受：577 / "577" / "第577条" / "第五百七十七条" / "第五百七十七条之一"。
    859 起另接受全角数字（第５７７条）与尾随款/项定位（第577条第2款——
    本工具只核验到条一级，款项定位被忽略，不因此报错）。
    """
    if isinstance(article, bool) or not isinstance(article, (int, str)):
        raise ValueError(
            f"条号类型不对：需要文本或整数，收到的是{_type_zh(article)}。"
            "下一步：请把条号写成如「第五百七十七条」或 577 再发。")
    if isinstance(article, int):
        return article, None
    s = _fw_norm(str(article or "").strip().replace("條", "条"))
    s = _ARTICLE_SUFFIX_RE.sub("", s)
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

# 无书名号引注（859 新增）：「根据民法典第五百七十七条」是中文法律
# 文本里同样常见的写法，857 版只认《》形式，这类引注会被整段核验
# 静默漏掉——编造的条文只要不写书名号就能绕过核验。法名用受限字集
# 拼（剔除虚词/动词字，避免把「根据」「依照」等吞进法名），且必须以
# 法规类后缀结尾；「本法/本条例」等自指称谓不算引注，另行剔除。
_BARE_NAME_FORBIDDEN = "根据依据按照依照关于对于向被将把与及或的了是在由从到给让这那其之说，。；、：:（）()《》「」『』\"'“”‘’\\s"
_BARE_CITATION_RE = re.compile(
    rf"([^{_BARE_NAME_FORBIDDEN}]{{1,30}}?法典"
    rf"|[^{_BARE_NAME_FORBIDDEN}]{{2,30}}?"
    r"(?:条例|规定|办法|细则|解释|规则|章程|通则|法))"
    r"\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")
_BARE_SELF_REFS = {"本法", "该法", "本条例", "本办法", "本规定", "本细则",
                   "本规则", "本章程", "该条例", "该办法"}


def extract_citations(text):
    """从整段文本中抽取引注：《法名》第N条 与无法名号的「法名第N条」。

    在全角数字/繁体「條」归一化后的副本上抽取（逐字 1:1 替换，
    span 与原文位置严格一致）；繁体法名本身不做简繁转换，抽得出、
    定位不到时由核验环节如实报「未检索到」，不静默漏检。
    """
    src = _fw_norm(text or "").replace("條", "条")
    out = []
    spans = []
    for m in _CITATION_RE.finditer(src):
        try:
            num = cn_to_int(m.group(2))
            sub = cn_to_int(m.group(3)) if m.group(3) else None
        except ValueError:
            continue
        out.append({"law": m.group(1), "article_num": num,
                    "article_sub": sub,
                    "article": article_heading(num, sub),
                    "span": [m.start(), m.end()]})
        spans.append((m.start(), m.end()))
    for m in _BARE_CITATION_RE.finditer(src):
        name = m.group(1).strip()
        # 句中连词粘连（如「…条和公司法第十六条」的「和公司法」）：
        # 法名不会以「和」开头，剥掉首字再用
        if name.startswith("和") and len(name) > 3:
            name = name[1:]
        if name in _BARE_SELF_REFS or name.startswith(("本", "该", "此")):
            continue
        if any(s <= m.start() < e or s < m.end() <= e for s, e in spans):
            continue  # 与书名号引注重叠，已由主模式抽出
        try:
            num = cn_to_int(m.group(2))
            sub = cn_to_int(m.group(3)) if m.group(3) else None
        except ValueError:
            continue
        out.append({"law": name, "article_num": num,
                    "article_sub": sub,
                    "article": article_heading(num, sub),
                    "span": [m.start(), m.end()]})
    out.sort(key=lambda c: c["span"][0])
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
    query = _arg_str(args, "query", required=True,
                     hint="下一步：请给出要检索的法名或关键词。").strip()
    if not query:
        raise SourceError("query 不能为空。下一步：请给出要检索的法名或关键词。")
    mode = _arg_str(args, "mode") or "fuzzy"
    if mode not in ("exact", "fuzzy"):
        raise SourceError(f"参数 mode 取值不对：只许 exact 或 fuzzy，收到「{mode}」。"
                          "下一步：请改用 exact（精确）或 fuzzy（模糊，默认）。")
    scope = _arg_str(args, "scope") or "title"
    if scope not in ("title", "body"):
        raise SourceError(f"参数 scope 取值不对：只许 title 或 body，收到「{scope}」。"
                          "下一步：请改用 title（只查标题，默认）或 body（查正文）。")
    limit = _arg_int(args, "limit", 10, 1, 20)
    source = _arg_str(args, "source") or "auto"
    if source not in ("auto", "flk", "moj", "gzk"):
        raise SourceError(f"参数 source 取值不对：只许 auto/flk/moj/gzk，"
                          f"收到「{source}」。下一步：请改用 auto（三库"
                          "合并，默认）、flk（国家法律法规数据库）、"
                          "moj（司法部行政法规库）或 gzk（国家规章库）。")
    results, totals, failures = [], {}, {}
    if source in ("auto", "flk"):
        try:
            total, rows = flk_search_rows(
                query,
                search_type=1 if mode == "exact" else 2,
                search_range=2 if scope == "body" else 1,
                page_size=limit,
            )
            totals["flk"] = total
            results += [dict(row_to_result(r), library="flk") for r in rows]
        except SourceError as e:
            if source == "flk":
                raise
            failures["flk"] = str(e)
    if source in ("auto", "moj"):
        try:
            mtotal, mrows = moj_search_rows(
                query, mode="content" if scope == "body" else "title",
                limit=limit)
            totals["moj"] = mtotal
            for r in mrows:
                code = _MOJ_STATUS_MAP.get(r.get("status", ""))
                if code is None:
                    status, status_en = (r.get("status") or "效力未核"), \
                        "unverified"
                else:
                    status, status_en, code = map_status(code)
                results.append({
                    "id": f"moj:{r['id']}", "title": r["title"],
                    "authority": "", "category": "行政法规",
                    "promulgated": r.get("promulgated", ""),
                    "effective_from": "", "status": status,
                    "status_en": status_en, "status_code": code,
                    "source_url": r["source_url"], "library": "moj",
                })
        except SourceError as e:
            if source == "moj":
                raise
            failures["moj"] = str(e)
    if source in ("auto", "gzk"):
        try:
            gtotal, grows = gzk_search_rows(
                query, mode="content" if scope == "body" else "title",
                limit=limit)
            totals["gzk"] = gtotal
            results += grows
        except SourceError as e:
            if source == "gzk":
                raise
            failures["gzk"] = str(e)
    if failures and not results:
        first = next(iter(failures.values()))
        raise SourceError(f"三个官方库本次均查询失败。{first}")
    notes = [f"{lib} 通道本次查询失败，结果不含该库：{err}"
             for lib, err in failures.items()]
    if source in ("auto", "moj") and "moj" not in failures \
            and scope == "title":
        idx = _moj_index()
        if idx["items"]:
            notes.append(
                f"司法部行政法规库的标题检索由本地标题索引接管"
                f"（覆盖 {len(idx['items'])} 部，构建于 "
                f"{idx['built_at']}）：该库服务端检索过滤未生效，"
                "索引保证标题覆盖全库；效力与原文仍以详情页实时"
                "取数为准")
            stale = _moj_index_stale_note()
            if stale:
                notes.append(stale)
        else:
            notes.append("司法部行政法规库的服务端检索过滤当前未生效"
                         "（2026-11-23 实测），且本地标题索引缺失，"
                         "moj 结果仅含其返回页内标题包含检索词的条目、"
                         "覆盖不全；该库详情通道可用，"
                         "verify/get_article 的 moj 兜底只对精确命中生效")
    return {
        "schema": RESULT_SCHEMA,
        "tool": "flk_search",
        "query": query,
        "total": len(results),
        "totals_by_library": totals,
        "results": results,
        "notes": notes,
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


# ---------------------------------------------------------------------------
# 条号沿革（renumber）建议（859 新增）：条号在现行版找不到时，
# 若旧版本里有此条，按旧条原文在现行版全文里找内容最接近的条，
# 给出「可能已改到第X条」的线索。相似度不够高就不臆测、返回 None。
# ---------------------------------------------------------------------------

_RENUMBER_MIN_SIMILARITY = 0.55


def _match_renumber(old_text, cur_paragraphs, num, sub):
    old_norm = _normalize_for_compare(old_text)
    if not old_norm:
        return None
    best = None
    for a in split_articles(cur_paragraphs):
        if a["num"] == num and (a["sub"] or None) == sub:
            continue
        ratio = difflib.SequenceMatcher(
            None, old_norm, _normalize_for_compare(a["text"])).ratio()
        if best is None or ratio > best[0]:
            best = (ratio, a)
    if best and best[0] >= _RENUMBER_MIN_SIMILARITY:
        return {"suggested_article": best[1]["heading"],
                "similarity": round(best[0], 3)}
    return None


def _renumber_suggestion_flk(law, num, sub, cache=None):
    try:
        details = fetch_law_details(law["id"])
    except SourceError:
        return None
    versions = [v for v in (details.get("lsyg") or [])
                if isinstance(v, dict) and v.get("bbbs")
                and v.get("bbbs") != law["id"]]
    versions.sort(key=lambda v: v.get("gbrq") or "", reverse=True)
    if not versions:
        return None
    try:
        cur_paras = _docx_paragraphs_cached(law["id"], cache)
    except SourceError:
        return None
    for v in versions[:2]:
        try:
            paras = _docx_paragraphs_cached(v["bbbs"], cache)
        except SourceError:
            continue
        found = extract_article(paras, num, sub)
        if not found:
            continue
        hit = _match_renumber(found["text"], cur_paras, num, sub)
        if hit is None:
            return None
        return {"from_version_title": clean_title(v.get("title")),
                "from_promulgated": v.get("gbrq") or "",
                "old_article": article_heading(num, sub), **hit}
    return None


def _low_sim_hedge(sug):
    """低相似度时的加重提醒（860）：阈值采样显示相似度 <0.75 的
    建议里混有「原条文已被删除、只是与某条现行条文貌似」的情况，
    建议口径必须随相似度收紧，不能一律说「很可能已调整」。"""
    if sug.get("similarity", 1) < 0.75:
        return ("；但相似度偏低，也存在原条文已被删除、并无对应"
                "现行条文的可能，请勿直接改引")
    return ""


def _renumber_note(sug):
    return (f"条号沿革提示：{sug['old_article']}见于旧版本"
            f"《{sug['from_version_title']}》"
            + (f"（{sug['from_promulgated']}公布）"
               if sug.get("from_promulgated") else "")
            + f"，其内容与现行文本{sug['suggested_article']}最接近"
              f"（相似度 {sug['similarity']}）——条号很可能已随修订"
              f"调整，引用时请改按现行条号{sug['suggested_article']}"
              "并逐字复核。" + _low_sim_hedge(sug))


def _relocated_suggestion(law_id, quoted_text, num, sub, cache=None):
    """内容去向提示（859）：条号还在、但所引文字与现行该条差异极大
    时，在现行全文里找与所引文字最接近的条——旧条文多半是随修订
    搬了家。相似度不够高不臆测。"""
    try:
        paras = _docx_paragraphs_cached(law_id, cache)
    except SourceError:
        return None
    hit = _match_renumber(quoted_text, paras, num, sub)
    if hit is None:
        return None
    return {"quoted_matches_current": hit["suggested_article"],
            "similarity": hit["similarity"]}


def _relocated_note(heading, sug):
    return (f"内容去向提示：所引文字与现行{heading}差异极大，但与现行"
            f"文本{sug['quoted_matches_current']}最接近"
            f"（相似度 {sug['similarity']}）——这段文字很可能已随修订"
            f"调整到{sug['quoted_matches_current']}，请改按现行条号引用"
            "并逐字复核。"
            + _low_sim_hedge({"similarity": sug["similarity"]}))


def _renumber_suggestion_moj(detail, num, sub):
    """moj 通道同款沿革建议：沿革版本走 moj 详情页全文比对。"""
    for h in (detail.get("history") or [])[:2]:
        try:
            old = moj_fetch_detail(h["id"])
        except SourceError:
            continue
        found = extract_article(old["paragraphs"], num, sub)
        if not found:
            continue
        hit = _match_renumber(found["text"], detail["paragraphs"],
                              num, sub)
        if hit is None:
            return None
        return {"from_version_title": h["title"], "from_promulgated": "",
                "old_article": article_heading(num, sub), **hit}
    return None


# ---------------------------------------------------------------------------
# 三库分工（857 补齐）：司法部行政法规库（moj）＋国家规章库（gzk）
#
# 分工口径（DESIGN_836）：flk 管法律/行政法规/司法解释的主干核验；
# moj 补行政法规的官方全文与效力（flk 未收录时兜底）；gzk 管部门规章/
# 地方政府规章的存在性与元数据（该库无官方效力字段、无条文全文，
# 只做到存在性层级，结论如实封顶并标注推断）。
# 纪律与 flk 通道同源：只发白名单域名、只 HTTPS、TLS 全程校验、
# 遇验证/拦截页立即终止；目标地址与网关密钥写死常量，不许经环境
# 变量改写（这正是 flk-mcp 允许环境变量覆盖网关配置的毛病，不继承）。
# ---------------------------------------------------------------------------

MOJ_BASE = "https://xzfg.moj.gov.cn"
MOJ_SOURCE_LABEL = "司法部国家行政法规库 xzfg.moj.gov.cn"
GZK_SOURCE_LABEL = "国家规章库 sousuoht.www.gov.cn（gov.cn 规章库）"
# gov.cn 规章库 athena 网关地址与前端公开的应用标识/RSA 公钥：均为
# 规章库网页前端代码内的公开明文（不是任何个人凭据），写死在此。
GZK_SEARCH_URL = ("https://sousuoht.www.gov.cn/athena/forward/"
                  "16BEFE295A368744B016A3B58F2EA7F3CF8A5946EE6CFB7EE98938"
                  "A538353125")
_GZK_APP_CODE = "18258ab0ac9"
_GZK_DATA_TYPE_ID = "92"
_GZK_APP_KEY_PLAIN = "f8f49ea85885466598c5261f7f8607fb"
_GZK_PUBLIC_KEY_B64 = (
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCWGTHvPbNkzQNxTJwSZbsgHKyLl/OK"
    "11kCZNmVVSFK3lUbmHgh7Ain1gdaf7G/ETh/wQm/9BAO/U36yWPizzlwHCUcWJXBRsY1"
    "0PsnYIlBXH/cjqQaEbmEghxcjdYtLtkudoMfoMDiJk+tPC7UEZd8TI2u26vttNF++6t"
    "Hi1HdeQIDAQAB")

_MOJ_STATUS_MAP = {"现行有效": 3, "已废止": 1, "已修改": 2,
                   "尚未生效": 4, "部分失效": 2}

# --- moj 标题索引（859 新增）：moj 检索端点的 Query 不生效、恒返
# 第一页，857 的兜底只能命中约 10 部。实测同一端点接受 PageIndex
# 分页，全库可遍历——tools/build_moj_index.py 离线抓成
# data/moj_index.json（标题→LawID 电话簿），标题检索与精确定位
# 改走索引，效力与原文仍以详情页实时取数为准。 ---
_MOJ_INDEX_CACHE = None


def _moj_index_path():
    env = os.environ.get("CN_LAW_VERIFY_MOJ_INDEX")
    if env:
        return env
    return os.path.join(os.path.dirname(snapshot_db_path()),
                        "moj_index.json")


def _moj_index():
    """读标题索引 → {"items": [...], "built_at": str|None}；无索引返回空。"""
    global _MOJ_INDEX_CACHE
    if _MOJ_INDEX_CACHE is not None:
        return _MOJ_INDEX_CACHE
    data = {"items": [], "built_at": None}
    try:
        with open(_moj_index_path(), encoding="utf-8") as fh:
            raw = json.load(fh)
        if isinstance(raw, dict) and isinstance(raw.get("items"), list):
            data = {
                "items": [i for i in raw["items"]
                          if isinstance(i, dict) and i.get("id")
                          and i.get("title")],
                "built_at": raw.get("built_at"),
            }
    except (OSError, ValueError):
        pass
    _MOJ_INDEX_CACHE = data
    return data


def _moj_index_items():
    return _moj_index()["items"]


# moj 标题索引新鲜度口径（860 新增）：索引是离线遍历的一次性
# 快照，新增/修订的行政法规不会自动进索引。此前 server 只在索引
# 缺失时出提示，索引过期时无声无息——超过阈值未重建时，检索结果
# 与健康检查都必须把「索引已旧」摆到明面上。
_MOJ_INDEX_STALE_DAYS = 30


def _moj_index_age_days():
    """索引构建距今的天数（浮点）；无索引或时间不可解析返回 None。"""
    built_at = _moj_index().get("built_at")
    if not built_at:
        return None
    try:
        built = datetime.fromisoformat(str(built_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if built.tzinfo is None:
        built = built.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - built).total_seconds() / 86400.0


def _moj_index_stale_note():
    """索引过期时的统一提示文案；未过期返回空串。"""
    age = _moj_index_age_days()
    if age is None or age <= _MOJ_INDEX_STALE_DAYS:
        return ""
    return (f"注意：本地标题索引已构建 {age:.0f} 天（超过 "
            f"{_MOJ_INDEX_STALE_DAYS} 天未重建），其间新公布或修订的"
            "行政法规可能不在索引内、标题检索会漏检；索引重建："
            "python3 tools/build_moj_index.py（离线遍历司法部库分页，"
            "串行礼貌抓取，约数分钟）")


def _wrap_alt_source_error(label, site_url, e):
    """moj/gzk 通道失败时的人话包装：截断 flk 专用下一步，换成本源口径。"""
    msg = str(e)
    head = msg.split("下一步：")[0].rstrip("。")
    head = head.replace("向官方源发请求这一步没走通：", "")
    return SourceError(
        f"{label}这一步没走通：{head}。下一步：①稍后重试；"
        f"②也可打开 {site_url} 人工核验；③属法律/司法解释层级的法规"
        "可改走国家法律法规数据库（flk）通道或本地快照。")


def _fetch_alt_text(url, label, site_url):
    try:
        # gzip 压缩后体积降一个量级，显著减少本机出口对大页面响应的截断
        raw = _request(url, retries=3, gzip_ok=True)
    except SourceError as e:
        raise _wrap_alt_source_error(label, site_url, e) from e
    text = raw.decode("utf-8", "replace")
    for hint in BLOCK_HINTS:
        if hint in text:
            raise SourceError(f"{label}返回了验证/拦截页，已终止查询；"
                              f"下一步：请稍后再试，或打开 {site_url} 人工核验")
    return text


# --- moj：检索列表解析（SearchAdvancedFront 的 incident-record 模板与
# SearchTitleFront 的 list-item-body 模板两种） ---

def _moj_parse_list(html_text):
    items, seen = [], set()

    def _add(law_id, title, status="", publish_date=""):
        title = clean_title(title)
        if not title or title in ("Word", "PDF", "查看更多"):
            return
        if law_id in seen:
            return
        seen.add(law_id)
        items.append({
            "id": law_id, "title": title, "status": status,
            "promulgated": publish_date,
            "source_url": f"{MOJ_BASE}/front/law/detail?LawID={law_id}",
            "library": "moj",
        })

    for block in re.finditer(
            r'<div[^>]*class=["\']incident-record["\'][^>]*>(.*?)</div>',
            html_text, re.DOTALL | re.IGNORECASE):
        m = re.search(r'<a[^>]*href=["\']([^"\']*LawID=(\d+)[^"\']*)["\']'
                      r'[^>]*>(.*?)</a>', block.group(1),
                      re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        tm = re.search(r'data-time=["\']([^"\']*)["\']', block.group(0))
        _add(m.group(2), m.group(3),
             publish_date=tm.group(1) if tm else "")
    if items:
        return items
    for block in re.finditer(
            r'<div[^>]*class=["\']list-item-body["\'][^>]*>.*?</div>\s*</div>',
            html_text, re.DOTALL | re.IGNORECASE):
        blk = block.group(0)
        m = re.search(r'<a[^>]*href=["\']([^"\']*LawID=(\d+)[^"\']*)["\']'
                      r'[^>]*>(.*?)</a>', blk, re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        status = ""
        sm = re.search(r'<li[^>]*class=["\']status["\'][^>]*>(.*?)</li>',
                       blk, re.DOTALL | re.IGNORECASE)
        if sm:
            status = clean_title(sm.group(1))
        pub = ""
        dm = re.search(r'<li[^>]*class=["\']publish-date["\'][^>]*>'
                       r'(.*?)</li>', blk, re.DOTALL | re.IGNORECASE)
        if dm:
            pub = clean_title(dm.group(1))
        _add(m.group(2), m.group(3), status=status, publish_date=pub)
    return items


def moj_search_rows(query, mode="title", limit=10):
    """司法部行政法规库检索 → (total, items)。

    title 模式优先走本地标题索引（859）：覆盖全库、精确在前；
    索引缺失时回退站点检索路径。content 模式仍走站点路径。
    实测警示（2026-11-23）：moj 三个检索端点当前均不按 Query 过滤，
    恒返回全库按日期升序的第一页——服务端过滤已失效。故回退路径的
    title 模式在本地按「标题包含检索词」二次过滤，绝不把未过滤的
    全库列表冒充检索结果；content 模式无法本地过滤，结果须人工复核。
    """
    if mode == "title":
        idx = _moj_index_items()
        if idx:
            def _dedupe(rows):
                best = {}
                for r in rows:
                    cur = best.get(r["title"])
                    if cur is None or int(r["id"]) > int(cur["id"]):
                        best[r["title"]] = r
                return list(best.values())

            exact = _dedupe([i for i in idx if i["title"] == query])
            contains = _dedupe([i for i in idx
                                 if query in i["title"]
                                 and i["title"] != query])
            merged = exact + contains
            out = [{
                "id": i["id"], "title": i["title"],
                "status": i.get("status", ""),
                "promulgated": i.get("promulgated", ""),
                "source_url": f"{MOJ_BASE}/front/law/detail?LawID={i['id']}",
                "library": "moj",
            } for i in merged]
            return len(out), out[:limit]
    endpoint = "SearchTitleFront" if mode == "title" else "SearchFront"
    params = {"SiteID": "122", "Query": query,
              "Type": "1" if mode == "title" else "2"}
    url = f"{MOJ_BASE}/{endpoint}?" + urllib.parse.urlencode(params)
    html_text = _fetch_alt_text(url, "司法部行政法规库检索", MOJ_BASE)
    items = _moj_parse_list(html_text)
    if not items and not re.search(
            r'class=["\'](incident-record|list-item-body)["\']',
            html_text, re.IGNORECASE):
        raise SourceError("司法部行政法规库返回的页面结构异常，解析不到"
                          "结果列表（官网可能改版）。下一步：请稍后重试，"
                          f"或打开 {MOJ_BASE} 人工核验")
    total = len(items)
    m = re.search(r'<input[^>]*(?:class|id)=["\']law-total["\'][^>]*'
                  r'value=["\'](\d+)["\']', html_text, re.IGNORECASE)
    if m:
        total = int(m.group(1))
    if mode == "title":
        items = [i for i in items if query in i["title"]]
    return total, items[:limit]


def _moj_status_from_html(html_text, fallback=""):
    m = re.search(r"(?:时效性|效力状态|当前状态)[：:\s]*"
                  r"(现行有效|已废止|已修改|尚未生效|部分失效)",
                  html_text)
    if m:
        return m.group(1)
    # 无显式标注时只在页面前段找状态词（避开页脚/筛选区的噪声），
    # 找不到就如实未核——绝不像 flk-mcp 那样兜底冒充「现行有效」。
    head = html_text[:6000]
    for word in ("已废止", "尚未生效", "已修改", "部分失效", "现行有效"):
        if word in head:
            return word
    return fallback


def _moj_dates_authority(paragraphs):
    head = "".join(paragraphs[:5])
    dates = re.findall(r"(\d{4}年\d{1,2}月\d{1,2}日)", head)
    promulgated = effective = ""
    pub_idx, eff_idx = head.find("公布"), head.find("起施行")
    if pub_idx > 0:
        before = [d for d in dates if head.find(d) < pub_idx]
        if before:
            promulgated = before[-1]
    if eff_idx > 0:
        before = [d for d in dates if head.find(d) < eff_idx]
        if before:
            effective = before[-1]
    authority = ""
    m = re.search(r"((?:中华人民共和国)?(?:国务院|各部|委员会|署|局|"
                  r"办公厅|办公室))", head)
    if m:
        authority = m.group(1)
    return promulgated, effective, authority


def moj_fetch_detail(law_id, fallback_status=""):
    """司法部行政法规库详情 → 元数据＋正文段落＋版本沿革。"""
    if not str(law_id).isdigit():
        raise SourceError("司法部行政法规库的 LawID 应为数字。"
                          "下一步：请先用 flk_search（source=moj）检索定位。")
    url = f"{MOJ_BASE}/front/law/detail?LawID={law_id}"
    html_text = _fetch_alt_text(url, "司法部行政法规库详情页", MOJ_BASE)
    title = ""
    m = re.search(r'<div[^>]*class=["\']text-title["\'][^>]*>(.*?)</div>',
                  html_text, re.DOTALL | re.IGNORECASE)
    if m:
        title = clean_title(m.group(1))
    if not title:
        raise SourceError("司法部行政法规库详情页解析不到标题"
                          "（官网可能改版）。下一步：请稍后重试，"
                          f"或打开 {url} 人工核验")
    paragraphs = []
    chapter = re.search(
        r'<div[^>]*class=["\']law-chapter["\'][^>]*>(.*?)</div>\s*</div>',
        html_text, re.DOTALL | re.IGNORECASE)
    if chapter:
        for p in re.findall(r"<p[^>]*>(.*?)</p>", chapter.group(1),
                            re.DOTALL | re.IGNORECASE):
            text = clean_title(p)
            if text:
                paragraphs.append(text)
    status_zh = _moj_status_from_html(html_text, fallback_status)
    history = []
    fold = re.search(
        r'<div[^>]*class=["\']fold-con-inner["\'][^>]*>(.*?)</div>\s*</div>',
        html_text, re.DOTALL | re.IGNORECASE)
    if fold:
        for hm in re.finditer(
                r'<a[^>]*href=["\'][^"\']*LawID=(\d+)[^"\']*["\'][^>]*>'
                r'(.*?)</a>', fold.group(1), re.DOTALL | re.IGNORECASE):
            htitle = clean_title(hm.group(2))
            if htitle and htitle not in ("Word", "PDF", "查看更多"):
                history.append({"id": hm.group(1), "title": htitle})
    promulgated, effective, authority = _moj_dates_authority(paragraphs)
    return {
        "id": str(law_id), "title": title, "paragraphs": paragraphs,
        "status_zh": status_zh,
        "status_code": _MOJ_STATUS_MAP.get(status_zh),
        "promulgated": promulgated, "effective_from": effective,
        "authority": authority, "history": history,
        "source_url": url,
    }


def _moj_law_dict(detail):
    code = detail["status_code"]
    if code is None:
        status, status_en = "效力未核（司法部页面未标注）", "unverified"
    else:
        status, status_en, code = map_status(code)
    return {
        "id": f"moj:{detail['id']}", "title": detail["title"],
        "authority": detail["authority"] or "",
        "category": "行政法规",
        "promulgated": detail["promulgated"] or "",
        "effective_from": detail["effective_from"] or "",
        "status": status, "status_en": status_en, "status_code": code,
        "source_url": detail["source_url"], "library": "moj",
    }


def _moj_stamp():
    return {"channel": "live",
            "source": f"{MOJ_SOURCE_LABEL}（官方源实时查询）",
            "fetched_at": datetime.now(timezone.utc).isoformat()}


def moj_find_law(name):
    """在司法部行政法规库按标题精确定位 → detail 或 None。

    有标题索引时按索引精确命中（同名多版本逐个取详情、优先现行
    有效版，与 flk find_law 同口径）；索引里没有即真没有，不再回
    退到只覆盖第一页的服务端检索。索引缺失时走旧检索路径。
    """
    name = (name or "").strip().strip("《》").strip()
    if not name:
        return None
    idx = _moj_index_items()
    if idx:
        cands = [i for i in idx if i["title"] == name]
        cands.sort(key=lambda i: int(i["id"]), reverse=True)
        fallback = None
        for item in cands[:3]:
            detail = moj_fetch_detail(
                item["id"], fallback_status=item.get("status", ""))
            if fallback is None:
                fallback = detail
            if detail["status_code"] == 3:
                return detail
            if fallback["status_code"] == 1 \
                    and detail["status_code"] != 1:
                fallback = detail
        return fallback
    _total, items = moj_search_rows(name, mode="title", limit=10)
    exact = [i for i in items if i["title"] == name]
    if not exact:
        return None
    return moj_fetch_detail(exact[0]["id"],
                            fallback_status=exact[0].get("status", ""))


# --- gzk：athena 网关（RSA 公钥加密的应用标识头） ---

_GZK_RSA_CACHE = None


def _der_tlv(data, i):
    """极简 DER TLV 读取 → (tag, value_bytes, next_index)。"""
    tag = data[i]
    i += 1
    first = data[i]
    i += 1
    if first & 0x80:
        nbytes = first & 0x7F
        length = int.from_bytes(data[i:i + nbytes], "big")
        i += nbytes
    else:
        length = first
    return tag, data[i:i + length], i + length


def _gzk_rsa_params():
    global _GZK_RSA_CACHE
    if _GZK_RSA_CACHE is not None:
        return _GZK_RSA_CACHE
    der = base64.b64decode(_GZK_PUBLIC_KEY_B64)
    _t, spki, _ = _der_tlv(der, 0)
    _t, _alg, j = _der_tlv(spki, 0)
    _t, bitstr, _j = _der_tlv(spki, j)
    _t, rsaseq, _ = _der_tlv(bitstr[1:], 0)  # 首字节为未用位数
    _t, n_bytes, k = _der_tlv(rsaseq, 0)
    _t, e_bytes, _k = _der_tlv(rsaseq, k)
    _GZK_RSA_CACHE = (int.from_bytes(n_bytes, "big"),
                      int.from_bytes(e_bytes, "big"))
    return _GZK_RSA_CACHE


def _gzk_app_key_header():
    """athenaAppKey：公开应用标识经 RSA PKCS#1 v1.5 加密后 Base64＋URL 编码。"""
    n, e = _gzk_rsa_params()
    plain = _GZK_APP_KEY_PLAIN.encode("utf-8")
    k = (n.bit_length() + 7) // 8
    ps_len = k - 3 - len(plain)
    if ps_len < 8:
        raise SourceError("国家规章库网关密钥长度异常，无法生成请求头")
    ps = bytes(b if b else 0xFF for b in os.urandom(ps_len))
    block = b"\x00\x02" + ps + b"\x00" + plain
    cipher = pow(int.from_bytes(block, "big"), e, n)
    return urllib.parse.quote(
        base64.b64encode(cipher.to_bytes(k, "big")).decode("ascii"),
        safe="")


# 中央国家机关名录（国务院组成部门/直属机构/部委管理的国家局等，
# 名录公开固定）：这些机关发布的规章就是部门规章。860 采样实测：
# 旧版只认「X令」令号字样，未判定样本 40/40 的发布机关都是中央
# 机关、机关字段本身俱全——是规则没用上字段，不是数据缺字段。
_GZK_CENTRAL_AUTHORITIES = frozenset({
    "外交部", "国防部", "国家发展和改革委员会", "教育部", "科学技术部",
    "工业和信息化部", "国家民族事务委员会", "公安部", "国家安全部",
    "民政部", "司法部", "财政部", "人力资源和社会保障部", "自然资源部",
    "生态环境部", "住房和城乡建设部", "交通运输部", "水利部",
    "农业农村部", "商务部", "文化和旅游部", "国家卫生健康委员会",
    "退役军人事务部", "应急管理部", "中国人民银行", "审计署",
    "国务院国有资产监督管理委员会", "海关总署", "国家税务总局",
    "国家市场监督管理总局", "国家金融监督管理总局", "中国证券监督管理委员会",
    "国家广播电视总局", "国家体育总局", "国家信访局", "国家统计局",
    "国家知识产权局", "国家国际发展合作署", "国家医疗保障局",
    "国务院参事室", "国家机关事务管理局", "国家认证认可监督管理委员会",
    "国家标准化管理委员会", "国家新闻出版署", "国家版权局",
    "国家宗教事务局", "国务院研究室", "国务院侨务办公室",
    "国务院港澳事务办公室", "国务院台湾事务办公室", "国家互联网信息办公室",
    "国家粮食和物资储备局", "国家能源局", "国家国防科技工业局",
    "国家烟草专卖局", "国家移民管理局", "国家林业和草原局",
    "国家铁路局", "中国民用航空局", "国家邮政局", "国家文物局",
    "国家中医药管理局", "国家疾病预防控制局", "国家矿山安全监察局",
    "国家消防救援局", "国家外汇管理局", "国家药品监督管理局",
    "中国银行保险监督管理委员会", "国家密码管理局", "国家保密局",
    "国家档案局", "国家公园管理局", "国务院办公厅", "国家数据局",
    "国家反垄断局", "全国人大常委会法制工作委员会",
})
# 地方前缀守卫：机关名以省/市/自治区等地名开头的是地方机关，
# 不许被中央后缀规则误吞（如「北京市教育委员会」）。
_GZK_PLACE_PREFIXES = (
    "北京", "天津", "上海", "重庆", "河北", "山西", "辽宁", "吉林",
    "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南",
    "湖北", "湖南", "广东", "海南", "四川", "贵州", "云南", "陕西",
    "甘肃", "青海", "台湾", "内蒙古", "广西", "西藏", "宁夏", "新疆",
    "香港", "澳门", "深圳", "广州", "南京", "杭州", "成都", "武汉",
    "西安", "厦门", "青岛", "大连", "宁波", "苏州", "长沙", "郑州",
    "济南", "沈阳", "哈尔滨", "长春", "昆明", "福州", "合肥", "南昌",
    "贵阳", "南宁", "海口", "兰州", "银川", "西宁", "乌鲁木齐",
    "呼和浩特", "拉萨", "石家庄", "太原", "无锡", "东莞", "佛山",
)


def _gzk_infer_level(authority, title, source=""):
    """规章层级推断（官方库无此字段）：结果恒标 inferred。"""
    text = " ".join([authority or "", title or "", source or ""])
    if "人民政府" in text or "政府网站" in text:
        return "地方政府规章"
    dept = ("部令", "总局令", "委员会令", "署令", "局令", "中国人民银行令")
    if any(k in text for k in dept) or re.search(r"(部|总局|委员会|署|行|局)令", text):
        return "部门规章"
    # 860 放宽：发布机关本身是中央国家机关 → 部门规章。国家规章库
    # 只收部门规章与地方政府规章两类，非人民政府发布的规章若出自
    # 中央机关，别无其他可能；机关名带地方前缀的先排除。
    auth = (authority or "").strip()
    if auth:
        first = re.split(r"[、，,；;／/\s]+", auth)[0].strip()
        if first in _GZK_CENTRAL_AUTHORITIES:
            return "部门规章"
        if not first.startswith(_GZK_PLACE_PREFIXES) and \
                re.search(r"(部|委员会|总局|总署|署|银行|办公厅|办公室)$",
                          first):
            return "部门规章"
    return "规章（层级未判定）"


def gzk_search_rows(query, mode="title", limit=10):
    """国家规章库检索 → (total, items)。只有元数据＋摘要，无全文无官方效力。"""
    body = {
        "code": _GZK_APP_CODE, "dataTypeId": _GZK_DATA_TYPE_ID,
        "searchWord": query, "pageNo": 1, "pageSize": min(limit, 20),
        "orderBy": "time", "searchBy": "content" if mode == "content" else "title",
        "granularity": "ALL",
        "customFilter": {"operator": "and", "properties": []},
    }
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "athenaAppKey": _gzk_app_key_header(),
        "athenaAppName": urllib.parse.quote("规章库", safe=""),
        "Referer": "https://www.gov.cn/zhengce/xxgk/gjgzk/index.htm",
    }
    try:
        raw = _request(GZK_SEARCH_URL,
                       data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                       headers=headers, retries=3)
    except SourceError as e:
        raise _wrap_alt_source_error(
            "国家规章库检索", "https://www.gov.cn/zhengce/xxgk/gjgzk/", e) from e
    try:
        data = json.loads(raw.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        raise SourceError("国家规章库网关响应不是合法 JSON（可能被拦截）。"
                          "下一步：请稍后重试，或打开 "
                          "https://www.gov.cn/zhengce/xxgk/gjgzk/ 人工核验")
    result = data.get("result") if isinstance(data, dict) else None
    rdata = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rdata, dict):
        rc = data.get("resultCode") if isinstance(data, dict) else None
        msg = rc.get("cnMsg") if isinstance(rc, dict) else "接口返回异常"
        raise SourceError(f"国家规章库网关业务错误：{msg}。"
                          "下一步：请稍后重试，或打开 "
                          "https://www.gov.cn/zhengce/xxgk/gjgzk/ 人工核验")
    middle = rdata.get("middle") or {}
    raw_list = middle.get("list") or []
    pager = rdata.get("pager") or {}
    total = pager.get("total") if isinstance(pager.get("total"), int) \
        else len(raw_list)

    def _join(v):
        if v is None:
            return ""
        if isinstance(v, list):
            return " ".join(str(x) for x in v if x is not None)
        return str(v)

    items = []
    for it in raw_list:
        if not isinstance(it, dict):
            continue
        title = clean_title(it.get("title_no_tag") or it.get("title") or "")
        if not title:
            continue
        agencies = _join(it.get("agencies")).strip() \
            or _join(it.get("source")).strip()
        items.append({
            "id": _join(it.get("documentId")).strip(),
            "title": title,
            "authority": agencies,
            "category": _gzk_infer_level(agencies, title,
                                         _join(it.get("source"))),
            "inferred": True,
            "promulgated": _join(it.get("time")).strip(),
            "effective_from": "",
            "status": "效力未核（国家规章库无官方效力字段）",
            "status_en": "unverified", "status_code": None,
            "source_url": it.get("url") or "",
            "summary": clean_title(it.get("content") or ""),
            "library": "gzk",
        })
    return total, items[:limit]


def _verify_against_moj(detail, law_name, article_in, citation_text,
                        _excerpt_ok=False, prior_notes=()):
    """三检（moj 通道）：对司法部行政法规库定位到的法规做同构核验。"""
    law = _moj_law_dict(detail)
    checks = {
        "existence": {
            "ok": True,
            "detail": f"已在司法部国家行政法规库定位到《{detail['title']}》"
                      "（国家法律法规数据库未收录，转此库核验）",
        },
    }
    code = detail["status_code"]
    status_ok = code == 3
    status_warn = code == 2
    if code is None:
        checks["status"] = {
            "ok": False, "warning": True, "status": law["status"],
            "detail": "司法部页面未标注效力状态，效力未核——引用前须"
                      "另行确认该法规现行有效",
        }
    else:
        checks["status"] = {
            "ok": status_ok, "warning": status_warn,
            "status": law["status"],
            "detail": (
                "该法规现行有效（司法部行政法规库标注）" if status_ok else
                "该法规已被修改：引用时须以现行文本为准" if status_warn else
                f"该法规当前效力状态为「{law['status']}」，"
                "不得作为现行有效依据引用"),
        }
    notes = list(prior_notes)
    article_result = None
    if article_in is not None and str(article_in).strip() != "":
        num, sub = normalize_article(article_in)
        heading = article_heading(num, sub)
        article_check = {"article": heading}
        found = extract_article(detail["paragraphs"], num, sub)
        if found is None:
            article_check.update({
                "ok": False,
                "detail": f"在《{detail['title']}》现行文本中未找到{heading}"
                          "（条号可能不存在、已漂移，或该条已在修订中调整）",
            })
            sug = _renumber_suggestion_moj(detail, num, sub)
            if sug:
                article_check["renumber_suggestion"] = sug
                notes.append(_renumber_note(sug))
            else:
                notes.append("若条号来自该法的旧版本文本：法规修订后条号可能"
                             "已经变更（renumber），可用 get_law 查看版本沿革，"
                             "再按现行文本的条号重新定位引用。")
        else:
            official_text = found["text"]
            article_check.update({
                "ok": True, "official_text": official_text,
                "text_source": "moj_official_text",
                "detail": f"已取回{heading}官方原文（司法部行政法规库）",
            })
            if citation_text:
                given = _normalize_for_compare(citation_text)
                official = _normalize_for_compare(official_text)
                exact = (given == official or given in official) \
                    if _excerpt_ok else given == official
                text_check = {"ok": exact, "exact_match": exact,
                              "compared_against": "moj_official_text",
                              "excerpt_mode": bool(_excerpt_ok)}
                if not exact:
                    ratio = difflib.SequenceMatcher(
                        None, given, official).ratio()
                    text_check["similarity"] = round(ratio, 4)
                    for i, (a, b) in enumerate(zip(given, official)):
                        if a != b:
                            text_check["first_diff_at"] = i
                            text_check["given_snippet"] = \
                                given[max(0, i - 10):i + 20]
                            text_check["official_snippet"] = \
                                official[max(0, i - 10):i + 20]
                            break
                    else:
                        text_check["first_diff_at"] = min(len(given),
                                                          len(official))
                    text_check["detail"] = ("引文与官方原文不逐字一致，"
                                            "差异见 first_diff_at 附近")
                    if ratio < 0.3:
                        hit = _match_renumber(citation_text,
                                              detail["paragraphs"],
                                              num, sub)
                        if hit:
                            rsug = {
                                "quoted_matches_current":
                                    hit["suggested_article"],
                                "similarity": hit["similarity"]}
                            text_check["relocated_suggestion"] = rsug
                            notes.append(_relocated_note(heading, rsug))
                else:
                    text_check["detail"] = "引文与官方原文逐字一致"
                checks["text"] = text_check
        checks["article"] = article_check
        article_result = article_check
    hard_fail = (code in (1, 4)
                 or (article_result is not None
                     and not article_result.get("ok"))
                 or (checks.get("text") is not None
                     and not checks["text"]["ok"]))
    if hard_fail:
        verdict, verdict_zh = "not_verified", "未通过核验"
    elif code is None or status_warn:
        verdict, verdict_zh = "exists_only", \
            "仅确认存在（效力未核或存在保留，见 checks）"
    else:
        verdict, verdict_zh = "verified", "已核验（存在性＋原文＋效力）"
    return {
        "schema": RESULT_SCHEMA, "tool": "verify_citation",
        "input": {"law": law_name, "article": article_in,
                  "citation_text_provided": bool(citation_text)},
        "law": law, "checks": checks,
        "verdict": verdict, "verdict_zh": verdict_zh,
        "notes": notes, "data_snapshot": _moj_stamp(),
    }


def _verify_via_other_libraries(law_name, article_in, citation_text,
                                _excerpt_ok=False):
    """flk 未收录时的三库兜底：moj 做条级三检；gzk 只做存在性（封顶）。

    两库都无精确命中时返回 None，由调用方按「不存在」口径出结论；
    通道故障不冒充「不存在」，记入 notes 后继续下一路。
    """
    prior = []
    detail = None
    try:
        detail = moj_find_law(law_name)
    except SourceError as e:
        prior.append(f"司法部行政法规库通道本次不可用：{e}")
    if detail is not None:
        return _verify_against_moj(detail, law_name, article_in,
                                   citation_text, _excerpt_ok, prior)
    exact = []
    try:
        _t, items = gzk_search_rows(law_name, mode="title", limit=10)
        name = (law_name or "").strip().strip("《》").strip()
        exact = [i for i in items if i["title"] == name]
    except SourceError as e:
        prior.append(f"国家规章库通道本次不可用：{e}")
    if exact:
        item = exact[0]
        return {
            "schema": RESULT_SCHEMA, "tool": "verify_citation",
            "input": {"law": law_name, "article": article_in,
                      "citation_text_provided": bool(citation_text)},
            "law": item,
            "checks": {
                "existence": {
                    "ok": True,
                    "detail": f"已在国家规章库定位到《{item['title']}》"
                              "（部门/地方政府规章层级，国家法律法规"
                              "数据库与司法部行政法规库均未收录）",
                },
                "status": {
                    "ok": False, "warning": True,
                    "status": item["status"],
                    "detail": "国家规章库没有官方效力字段，效力未核；"
                              "该库也不提供条文全文，条文原文与引文"
                              "逐字比对无法在此通道完成",
                },
            },
            "verdict": "exists_only",
            "verdict_zh": "仅确认存在（规章层级：效力未核、无条文全文可比对）",
            "notes": prior + [
                "下一步：请打开官方链接核对该规章的现行全文与效力，"
                "或向制定机关官网复核后再引用。"],
            "data_snapshot": {
                "channel": "live",
                "source": f"{GZK_SOURCE_LABEL}（官方源实时查询）",
                "fetched_at": datetime.now(timezone.utc).isoformat()},
        }
    if prior:
        # 两库都故障时不许把「查不到」说成「不存在」
        raise SourceError("国家法律法规数据库未收录该法规，且兜底通道"
                          "本次也不可用：" + "；".join(prior))
    return None


def _get_law_via_moj(name):
    """get_law 的 moj 兜底：返回与 get_law 同构的结果（无官方目录树）。"""
    try:
        detail = moj_find_law(name)
    except SourceError:
        return None
    if detail is None:
        return None
    law = _moj_law_dict(detail)
    articles = split_articles(detail["paragraphs"])
    versions = [{
        "id": law["id"], "title": detail["title"],
        "promulgated": detail["promulgated"] or "",
        "is_current": True, "source_url": detail["source_url"],
    }]
    for h in detail["history"]:
        versions.append({
            "id": f"moj:{h['id']}", "title": h["title"],
            "promulgated": "", "is_current": False,
            "source_url": f"{MOJ_BASE}/front/law/detail?LawID={h['id']}",
        })
    return {
        "schema": RESULT_SCHEMA, "tool": "get_law",
        "law": law,
        "article_count": len(articles) if articles else None,
        "article_count_note": ("条数由司法部详情页全文切条计数"
                               "（该法规未被国家法律法规数据库收录，"
                               "无官方目录树可对）" if articles else
                               "司法部详情页未取到正文段落，条数未能统计"),
        "outline": [],
        "versions": versions,
        "versions_note": ("版本沿革来自司法部详情页「历史沿革」区；"
                          "沿革各版的公布日期该页未标注，须打开对应"
                          "链接核对" if len(versions) > 1 else
                          "司法部详情页未提供历史沿革（该库中仅此一版）"),
        "related_materials": [],
        "data_snapshot": _moj_stamp(),
    }


def _get_article_via_moj(law_name, num, sub, heading):
    """get_article 的 moj 兜底：flk 未收录时从司法部库取条文原文。"""
    try:
        detail = moj_find_law(law_name)
    except SourceError:
        return None
    if detail is None:
        return None
    found = extract_article(detail["paragraphs"], num, sub)
    if found is None:
        sug = _renumber_suggestion_moj(detail, num, sub)
        if sug:
            raise SourceError(
                f"司法部行政法规库收录了《{detail['title']}》，但其中未"
                f"找到{heading}。" + _renumber_note(sug))
        raise SourceError(
            f"司法部行政法规库收录了《{detail['title']}》，但其中未找到"
            f"{heading}（条号可能不存在或已漂移；若条号来自旧版本，"
            "修订后条号可能已变更，可用 get_law 查看版本沿革后按现行"
            "条号重新定位）")
    law = _moj_law_dict(detail)
    notes = ["原文取自司法部国家行政法规库官方详情页全文"
             "（国家法律法规数据库未收录该法规，转此库取回）"]
    if law["status_code"] == 4:
        notes.append("注意：该法规尚未生效，不得作为现行有效依据引用")
    elif law["status_code"] == 1:
        notes.append("注意：该法规已废止，不得作为现行有效依据引用")
    elif law["status_code"] is None:
        notes.append("注意：司法部页面未标注效力状态，效力未核，"
                     "引用前请另行确认现行有效")
    return {
        "schema": RESULT_SCHEMA, "tool": "get_article",
        "law": law, "article": heading, "text": found["text"],
        "text_source": "moj_official_text", "notes": notes,
        "data_snapshot": _moj_stamp(),
    }


def _verify_citation_live(args, _cache=None, _excerpt_ok=False):
    """三检（实时通道）：存在性 / 效力 / 条文原文（给了引文再做逐字比对）。

    _excerpt_ok：引文是整段文本中抽出的节录时置 True——节录与官方原文
    做包含比对（节录须逐字落在原文内），不要求与整条全文等同。
    """
    law_name = _arg_str(args, "law", required=True,
                        hint="下一步：请给出要核验的法规全名，"
                             "如「中华人民共和国民法典」。").strip()
    article_in = args.get("article")
    if article_in is not None and (isinstance(article_in, bool)
                                   or not isinstance(article_in,
                                                     (str, int))):
        raise SourceError(
            f"参数 article 类型不对：需要文本或整数，收到的是"
            f"{_type_zh(article_in)}。下一步：请把条号写成如"
            "「第五百七十七条」或 577 再发。")
    citation_text = (args.get("citation_text") or args.get("text")
                     or args.get("quoted_text"))
    if citation_text is not None and not isinstance(citation_text, str):
        raise SourceError(
            f"参数 citation_text 类型不对：需要文本，收到的是"
            f"{_type_zh(citation_text)}。下一步：请把待核验的引文原文"
            "以文本形式给出。")
    if not law_name:
        raise SourceError("law（法名）不能为空。下一步：请给出要核验的"
                          "法规全名，如「中华人民共和国民法典」。")

    checks = {}
    law, match = find_law(law_name)
    if law is None:
        # 三库分工兜底（857）：flk 未收录 → moj 条级三检 → gzk 存在性
        alt = _verify_via_other_libraries(law_name, article_in,
                                          citation_text,
                                          _excerpt_ok=_excerpt_ok)
        if alt is not None:
            return alt
        checks["existence"] = {"ok": False,
                               "detail": f"国家法律法规数据库、司法部行政法规库"
                                         f"与国家规章库中均未检索到《{law_name}》"}
        return {
            "schema": RESULT_SCHEMA, "tool": "verify_citation",
            "input": {"law": law_name, "article": article_in},
            "law": None, "checks": checks, "verdict": "not_verified",
            "verdict_zh": "未通过核验",
            "notes": ["三个官方库均未收录：法规本身不存在或名称有误，"
                      "后续检查无从谈起；请核对法名后重试，"
                      "或改用 flk_search 先检索。"],
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
    name_warn = law.get("name_mismatch")
    if name_warn:
        notes.append(
            f"法名不完全一致：所给法名「{law_name}」比定位到的法规"
            f"《{law['title']}》多出「{name_warn}」，可能实际指向另一部"
            "同名衍生法规；本次结论只针对定位到的法规，总判定已降级，"
            "引用前请先确认法规本身无误。")
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
            sug = _renumber_suggestion_flk(law, num, sub, cache=_cache)
            if sug:
                article_check["renumber_suggestion"] = sug
                notes.append(_renumber_note(sug))
            else:
                notes.append("若条号来自该法的旧版本文本：法规修订后条号可能"
                             "已经变更（renumber），可用 get_law 查看版本沿革，"
                             "再按现行文本的条号重新定位引用。")
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
                if _excerpt_ok:
                    # 节录比对：节录须逐字落在官方原文之内（多出原文之外
                    # 的内容即不通过），不要求等于整条全文
                    exact = given == official or given in official
                else:
                    exact = given == official or (
                        text_source == "official_context_window"
                        and (given in official or official in given))
                text_check = {"ok": exact, "exact_match": exact,
                              "compared_against": text_source,
                              "excerpt_mode": bool(_excerpt_ok)}
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
                    if ratio < 0.3:
                        # 差异极大不像笔误，更像条文已随修订搬家：
                        # 在现行全文里指出所引文字的实际去向（859）
                        rsug = _relocated_suggestion(
                            law["id"], citation_text, num, sub,
                            cache=_cache)
                        if rsug:
                            text_check["relocated_suggestion"] = rsug
                            notes.append(_relocated_note(heading, rsug))
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
    degraded = (status_warn or bool(name_warn)
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


def _verify_citation_snapshot(law_name, article_in, citation_text,
                              _excerpt_ok=False):
    """三检（快照通道）：只查本地快照库，不碰网络。

    快照未收录该法时返回 None（由调用方决定报错口径）；收录了则给出
    与实时通道同构的三检结论，通道与快照构建时间如实标注。
    """
    info = snapshot_info()
    if info is None:
        return None
    law = snapshot_find_law(law_name)
    if law is None:
        return None
    checks = {
        "existence": {
            "ok": True,
            "detail": f"已在本地快照库定位到《{law['title']}》"
                      f"（快照构建于 {info['built_at']}）",
        },
    }
    status_ok = law["status_code"] == 3
    status_warn = law["status_code"] == 2
    checks["status"] = {
        "ok": status_ok,
        "warning": status_warn,
        "status": law["status"],
        "detail": (
            "该法规在快照构建时为现行有效" if status_ok else
            "该法规已被修改：快照收录的为修改后版本，引用时须以现行文本为准"
            if status_warn else
            f"该法规在快照构建时的效力状态为「{law['status']}」，"
            "不得作为现行有效依据引用"),
    }
    notes = []
    if law.get("name_mismatch"):
        notes.append(
            f"法名不完全一致：所给法名「{law_name}」比快照定位到的法规"
            f"《{law['title']}》多出「{law['name_mismatch']}」，可能实际"
            "指向另一部同名衍生法规；本次结论只针对定位到的法规，"
            "总判定已降级，引用前请先确认法规本身无误。")
    article_result = None
    if article_in is not None and str(article_in).strip() != "":
        num, sub = normalize_article(article_in)
        heading = article_heading(num, sub)
        article_check = {"article": heading}
        art = snapshot_get_article(law["id"], num, sub)
        if art is None:
            article_check.update({
                "ok": False,
                "detail": f"在快照收录的《{law['title']}》文本中未找到{heading}"
                          "（条号可能不存在、已漂移，或该条已在修订中调整）",
            })
            notes.append("若条号来自该法的旧版本文本：法规修订后条号可能"
                         "已经变更（renumber），可用 get_law 查看版本沿革，"
                         "再按现行文本的条号重新定位引用。")
        else:
            official_text = art["text"]
            article_check.update({
                "ok": True,
                "official_text": official_text,
                "text_source": "local_snapshot",
                "detail": f"已取回{heading}快照原文",
            })
            if citation_text:
                given = _normalize_for_compare(citation_text)
                official = _normalize_for_compare(official_text)
                exact = (given == official or given in official) \
                    if _excerpt_ok else given == official
                text_check = {"ok": exact, "exact_match": exact,
                              "compared_against": "local_snapshot",
                              "excerpt_mode": bool(_excerpt_ok)}
                if not exact:
                    ratio = difflib.SequenceMatcher(
                        None, given, official).ratio()
                    text_check["similarity"] = round(ratio, 4)
                    for i, (a, b) in enumerate(zip(given, official)):
                        if a != b:
                            text_check["first_diff_at"] = i
                            text_check["given_snippet"] = \
                                given[max(0, i - 10):i + 20]
                            text_check["official_snippet"] = \
                                official[max(0, i - 10):i + 20]
                            break
                    else:
                        text_check["first_diff_at"] = min(len(given),
                                                          len(official))
                    text_check["detail"] = ("引文与快照原文不逐字一致，"
                                            "差异见 first_diff_at 附近")
                else:
                    text_check["detail"] = "引文与快照原文逐字一致"
                checks["text"] = text_check
        checks["article"] = article_check
        article_result = article_check
    hard_fail = (law["status_code"] in (1, 4)
                 or (article_result is not None
                     and not article_result.get("ok"))
                 or (checks.get("text") is not None
                     and not checks["text"]["ok"]))
    if hard_fail:
        verdict, verdict_zh = "not_verified", "未通过核验"
    elif status_warn or law.get("name_mismatch"):
        verdict, verdict_zh = "exists_only", \
            "仅确认存在（效力或法名存在保留，见 checks）"
    else:
        verdict, verdict_zh = "verified", "已核验（存在性＋原文＋效力）"
    return {
        "schema": RESULT_SCHEMA, "tool": "verify_citation",
        "input": {"law": law_name, "article": article_in,
                  "citation_text_provided": bool(citation_text)},
        "law": law, "checks": checks,
        "verdict": verdict, "verdict_zh": verdict_zh,
        "notes": notes,
        "data_snapshot": _snapshot_stamp("snapshot",
                                         as_of=info["built_at"]),
    }


def tool_verify_citation(args, _cache=None, _excerpt_ok=False):
    """三检（双通道）：实时通道优先；实时不可用时自动降级快照通道。

    降级口径（设计正本）：结论标注通道与快照构建时间，绝不把快照结论
    冒充实时结果；快照也未收录时，把实时通道的人话错误原样上报。
    """
    try:
        return _verify_citation_live(args, _cache=_cache,
                                     _excerpt_ok=_excerpt_ok)
    except SourceError as live_err:
        # 参数类错误不是通道故障，不许被降级掩盖
        msg = str(live_err)
        if "类型不对" in msg or "不能为空" in msg or "缺少必填参数" in msg:
            raise
        law_name = (args.get("law") or "").strip() \
            if isinstance(args.get("law"), str) else ""
        article_in = args.get("article")
        citation_text = (args.get("citation_text") or args.get("text")
                         or args.get("quoted_text"))
        snap = None
        if law_name:
            try:
                snap = _verify_citation_snapshot(
                    law_name, article_in, citation_text,
                    _excerpt_ok=_excerpt_ok)
            except (ValueError, SourceError):
                snap = None
        if snap is not None:
            snap["degraded"] = True
            snap["notes"] = [
                f"实时通道本次不可用（{msg}），已自动降级到本地快照通道："
                "以下结论基于快照数据（构建时间见 data_snapshot），"
                "效力状态以快照构建时的官方标注为准；联网恢复后建议"
                "再走一次实时核验。"] + (snap.get("notes") or [])
            return snap
        raise


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


def _like_escape(term):
    return (term.replace("\\", "\\\\").replace("%", "\\%")
            .replace("_", "\\_"))


def snapshot_find_law(name):
    """在快照库中按法名定位（精确 → 简称包含，同名优先现行有效版）。

    859 性能修复：候选过滤下推 SQL（857 版每次全表载入 laws 在
    Python 里逐行比对，快照涨到千部级后纯属浪费）。
    """
    conn = _open_snapshot()
    if conn is None:
        return None
    name = (name or "").strip().strip("《》").strip()
    if not name:
        conn.close()
        return None
    try:
        rows = conn.execute("SELECT * FROM laws WHERE title = ?",
                            (name,)).fetchall()
        exact = bool(rows)
        if not rows:
            rows = conn.execute(
                "SELECT * FROM laws WHERE title LIKE ? ESCAPE '\\' "
                "OR instr(?, title) > 0",
                ("%" + _like_escape(name) + "%", name)).fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    rows.sort(key=lambda r: (_STATUS_PREFERENCE.get(r["status_code"], 9),
                             abs(len(r["title"]) - len(name))))
    chosen = _snapshot_law_dict(rows[0])
    if not exact:
        rest = _name_mismatch_rest(name, chosen["title"])
        if rest:
            chosen["name_mismatch"] = rest
    return chosen


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
    """快照库条文检索：同一条文须同时含全部关键词（AND），按命中次数排序。

    859 性能修复：关键词过滤下推 SQL LIKE（857 版把全部条文载入
    Python 逐条做子串扫描，165k 条规模下单次 3–6 秒；下推后只把
    命中行取回打分）。关键词中的 %/_ 按字面量转义，不当通配符。
    """
    conn = _open_snapshot()
    if conn is None:
        return None
    where = " AND ".join("a.text LIKE ? ESCAPE '\\'" for _ in terms)
    params = ["%" + _like_escape(t) + "%" for t in terms]
    try:
        rows = conn.execute(
            "SELECT a.bbbs, a.num, a.sub, a.heading, a.text, "
            "l.title, l.status_code FROM articles a "
            f"JOIN laws l ON l.bbbs = a.bbbs WHERE {where}",
            params).fetchall()
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
    bbbs = _arg_str(args, "id").strip()
    if bbbs:
        law = {"id": bbbs}
    else:
        name = (_arg_str(args, "law") or _arg_str(args, "query")).strip()
        if not name:
            raise SourceError("get_law 需要 id（bbbs）或 law（法名）。"
                              "下一步：请给出法规全名，如「中华人民共和国公司法」。")
        found, _match = find_law(name)
        if found is None:
            moj_out = _get_law_via_moj(name)
            if moj_out is not None:
                return moj_out
            raise SourceError(
                f"国家法律法规数据库与司法部行政法规库中均未检索到"
                f"《{name}》。下一步：请核对法名；部门规章/地方政府规章"
                "请用 flk_search（source=gzk）在国家规章库中检索。")
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
        if snap_law is not None and not snap_law.get("name_mismatch"):
            article_count = snap_law["article_count"]
            count_note = "官方目录树未细化到条，条数取自本地快照切条计数"
        elif snap_law is not None:
            # 860 修复：快照只按包含关系定位到名称相近的另一部法
            # （如法律解释的标题里含被解释法律的法名）时，那部法的
            # 条数与本法无关——旧版照单全收，曾把《刑法》的 505 条
            # 记到其法律解释名下。名称不完全一致就不借用条数。
            article_count = None
            count_note = ("官方目录树未细化到条；本地快照未收录该法"
                          "（仅有名称相近的其他法规），条数未能统计")
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
    law_name = _arg_str(args, "law").strip()
    bbbs_in = _arg_str(args, "id").strip()
    article_in = args.get("article")
    if article_in is not None and (isinstance(article_in, bool)
                                   or not isinstance(article_in,
                                                     (str, int))):
        raise SourceError(
            f"参数 article 类型不对：需要文本或整数，收到的是"
            f"{_type_zh(article_in)}。下一步：请把条号写成如"
            "「第五百七十七条」或 577 再发。")
    if article_in is None or str(article_in).strip() == "":
        raise SourceError("article（条号）不能为空。下一步：请给出条号，"
                          "如「第五百七十七条」或 577。")
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
            snap_notes = ["原文取自本地快照库（官方 DOCX 全文切条），"
                          "效力状态以快照构建时官方标注为准"]
            if snap_law.get("name_mismatch"):
                snap_notes.append(
                    f"法名不完全一致：所给法名比定位到的法规多出"
                    f"「{snap_law['name_mismatch']}」，可能实际指向另一部"
                    "同名衍生法规；以下原文属于定位到的法规，引用前"
                    "请先确认法规本身无误。")
            if snap_law["status_code"] == 4:
                snap_notes.append("注意：该法规尚未生效（快照构建时官方"
                                  "标注），不得作为现行有效依据引用")
            elif snap_law["status_code"] == 1:
                snap_notes.append("注意：该法规已废止（快照构建时官方"
                                  "标注），不得作为现行有效依据引用")
            return {
                "schema": RESULT_SCHEMA, "tool": "get_article",
                "law": snap_law, "article": heading,
                "text": art["text"], "text_source": "local_snapshot",
                "notes": snap_notes,
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
            moj_out = _get_article_via_moj(law_name, num, sub, heading)
            if moj_out is not None:
                moj_out["notes"] = notes + moj_out["notes"]
                return moj_out
            raise SourceError(
                f"国家法律法规数据库与司法部行政法规库中均未检索到"
                f"《{law_name}》。下一步：请核对法名；部门规章/地方政府"
                "规章请用 flk_search（source=gzk）在国家规章库中检索"
                "确认是否存在。")
    text, text_source, fetch_notes = fetch_official_article(law, num, sub)
    notes.extend(fetch_notes)
    if text is None:
        sug = _renumber_suggestion_flk(law, num, sub)
        if sug:
            raise SourceError(
                f"在《{law['title']}》现行文本中未找到{heading}。"
                + _renumber_note(sug))
        raise SourceError(f"在《{law['title']}》现行文本中未找到{heading}"
                          "（条号可能不存在或已漂移；若条号来自旧版本，"
                          "修订后条号可能已变更，可用 get_law 查看版本"
                          "沿革后按现行条号重新定位）")
    if law.get("status_code") == 4:
        notes.append("注意：该法规尚未生效，不得作为现行有效依据引用")
    elif law.get("status_code") == 1:
        notes.append("注意：该法规已废止，不得作为现行有效依据引用")
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
    keywords = _arg_str(args, "keywords", required=True,
                        hint="下一步：请给出要检索的关键词，"
                             "多个关键词以空格分隔。").strip()
    terms = _split_terms(keywords)
    limit = _arg_int(args, "limit", 10, 1, 30)
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

_QUOTE_PAIRS = {'"': '"', "“": "”", "「": "」", "『": "』",
                "'": "'", "‘": "’"}
_QUOTE_STOP_CHARS = "。；！？\n"


# 引文抽取上限：错配的引号对会把大段无关文本吞成「引文」（859
# 实测 1003 字）。2500 字覆盖绝大多数整条引文；残余的短错配靠
# 比对环节包含率兜底，后果方向是保守的未通过，不会误报通过。
_MAX_QUOTED_LEN = 2500


def _extract_following_quote(text, pos, max_scan=48):
    """抽取引注后紧跟的引号内引文（如：第N条规定：「……」）。

    在引注匹配结束后的 max_scan 字内找第一组引号；中途遇到句末标点
    或下一处《法名》引注即停。引文过短（<6 字）不作为比对对象。
    找不到返回 None（该条只做存在性＋效力核验，结论中如实注明）。
    859 加固：闭引号搜索设上限——超过 _MAX_QUOTED_LEN 字、或中途
    已经出现下一处引注的，视为引号不配对，不把大半篇文章吞作
    「引文」去比对（857 版无上限，曾吞入 1000+ 字无关文本）。
    """
    seg = text[pos:pos + max_scan]
    for i, ch in enumerate(seg):
        if ch in _QUOTE_PAIRS:
            close = _QUOTE_PAIRS[ch]
            end = text.find(close, pos + i + 1,
                            pos + i + 1 + _MAX_QUOTED_LEN)
            if end == -1:
                return None
            if _CITATION_RE.search(text, pos + i + 1, end):
                return None
            quoted = text[pos + i + 1:end].strip()
            return quoted if len(quoted) >= 6 else None
        if ch in _QUOTE_STOP_CHARS or ch == "《":
            return None
    return None


_CLOSE_TO_OPEN = {v: k for k, v in _QUOTE_PAIRS.items()}


def _extract_preceding_quote(text, pos, claimed=()):
    """抽取引注之前紧邻的引号引文（860 新增）：「条文原文」——
    《X法》第N条、先引后注是中文法律文书的常见行文，859 版只认
    引注后引文，这类整段漏比对（860 采样 6 例全漏）。

    约束（防误关联）：引文闭引号与引注之间隔 ≤48 字且不跨句末
    标点；引文内不含其他引注；不与已被后置抽取认领的引文重叠；
    引号错配/嵌套不臆测。关联错了后果也只是逐字比对不过（保守
    方向），不会误报通过。
    """
    start = max(0, pos - 48)
    close_idx, close_ch = -1, None
    for i in range(pos - 1, start - 1, -1):
        ch = text[i]
        if ch in _QUOTE_STOP_CHARS:
            return None
        if ch in _CLOSE_TO_OPEN:
            close_idx, close_ch = i, ch
            break
    if close_idx < 0:
        return None
    open_ch = _CLOSE_TO_OPEN[close_ch]
    open_idx = text.rfind(open_ch, max(0, close_idx - _MAX_QUOTED_LEN),
                          close_idx)
    if open_idx < 0:
        return None
    inner = text[open_idx + 1:close_idx]
    if close_ch in inner:
        return None
    if _CITATION_RE.search(inner) or _BARE_CITATION_RE.search(inner):
        return None
    if any(open_idx < e and close_idx >= s for s, e in claimed):
        return None
    quoted = inner.strip()
    return quoted if len(quoted) >= 6 else None


def tool_check_citations_in_text(args):
    text = _arg_str(args, "text", required=True,
                    hint="下一步：请把待核验的整段文本以文本形式给出。")
    if not text.strip():
        raise SourceError("text 不能为空。下一步：请把待核验的整段文本"
                          "粘贴进来再发。")
    found = extract_citations(text)
    # 去重键必须含引文（859 修复）：同一条被引两次、引文一真一假时，
    # 857 版按（法名，条号）去重只检第一处，伪造的第二处引文会被
    # 静默放行——整段核验的汇总结论随之失真。
    seen, unique = set(), []
    claimed = []
    for c in found:
        quoted = _extract_following_quote(text, c["span"][1])
        qpos = "after" if quoted else None
        if quoted:
            qs = text.find(quoted, c["span"][1],
                           c["span"][1] + 50 + len(quoted))
            if qs >= 0:
                claimed.append((qs, qs + len(quoted)))
        if not quoted:
            # 后置没有再试前置（860）：先引后注的行文同样要比对
            quoted = _extract_preceding_quote(text, c["span"][0],
                                              claimed)
            if quoted:
                qpos = "before"
                qs = text.rfind(
                    quoted,
                    max(0, c["span"][0] - _MAX_QUOTED_LEN - 50),
                    c["span"][0])
                if qs >= 0:
                    claimed.append((qs, qs + len(quoted)))
        key = (c["law"], c["article_num"], c["article_sub"], quoted or "")
        if key not in seen:
            seen.add(key)
            unique.append(dict(c, _quoted=quoted, _qpos=qpos))
    truncated = len(unique) > _MAX_CITATIONS_PER_RUN
    to_check = unique[:_MAX_CITATIONS_PER_RUN]
    cache = {}
    results = []
    text_checked_count = 0
    for c in to_check:
        entry = {"law": c["law"], "article": c["article"],
                 "span": c["span"]}
        # 引注后紧跟的引号引文 → 一并做逐字比对（与 verify_citation
        # 同一核验内核，节录模式）；没有引号引文则只验存在与效力，
        # 并在条目里如实注明未做文本比对
        quoted = c.get("_quoted")
        entry["quoted_text_found"] = bool(quoted)
        if quoted:
            entry["quote_position"] = c.get("_qpos") or "after"
        try:
            vargs = {"law": c["law"], "article": c["article"]}
            if quoted:
                vargs["citation_text"] = quoted
            out = tool_verify_citation(vargs, _cache=cache,
                                       _excerpt_ok=bool(quoted))
            entry["verdict"] = out["verdict"]
            entry["verdict_zh"] = out["verdict_zh"]
            entry["law_matched"] = out["law"]["title"] if out.get("law") else None
            entry["status"] = out["law"]["status"] if out.get("law") else None
            entry["source_url"] = out["law"]["source_url"] if out.get("law") else None
            entry["channel"] = (out.get("data_snapshot") or {}).get("channel")
            problems = []
            for key, chk in (out.get("checks") or {}).items():
                if isinstance(chk, dict) and chk.get("ok") is False:
                    problems.append(f"{key}：{chk.get('detail', '')}")
            entry["problems"] = problems
            art_chk = (out.get("checks") or {}).get("article") or {}
            if art_chk.get("official_text"):
                entry["official_text"] = art_chk["official_text"]
                entry["text_source"] = art_chk.get("text_source")
            text_chk = (out.get("checks") or {}).get("text")
            if text_chk is not None:
                text_checked_count += 1
                entry["text_check"] = {
                    "exact_match": text_chk.get("exact_match"),
                    "compared_against": text_chk.get("compared_against"),
                    "detail": text_chk.get("detail", ""),
                }
            elif quoted:
                entry["text_check"] = {
                    "exact_match": None,
                    "detail": "已抽到引号引文，但条文原文未取回，未能比对",
                }
            else:
                entry["text_check"] = {
                    "exact_match": None,
                    "detail": "引注后未找到引号引文，本条未做逐字比对"
                              "（只验了存在性与效力）",
                }
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
    notes.append(f"其中 {text_checked_count} 条带引号引文、已做逐字比对；"
                 "未带引文的条目只验了存在性与效力（见各条 text_check）")
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
    q = _fw_norm((query or "").strip()).replace("條", "条")
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
    query = _arg_str(args, "query", required=True,
                     hint="下一步：请用一句话给出要查的法规或条文，"
                          "如「民法典577条」。").strip()
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
                       "否定极性冲突、排他性限定、条件偷换、单位偷换、"
                       "主体偷换、数量边界方向八类信号），只是初筛，"
                       "不是语义判定，不能替代人工判断；引注是否真正"
                       "支持论断，请结合条文全文与案件事实人工复核。")

_NEG_WORDS = ("不得", "不能", "不应", "禁止", "不准", "无需", "无须",
              "不承担", "不予", "不会", "没有", "不发生",
              "不产生", "不适用", "不影响", "不限制")
_POS_WORDS = ("应当", "必须", "有权", "可以", "承担", "赔偿", "履行",
              "有权要求", "应当承担")
# 极性冲突用定向模式：裸字「不」会把引文里描述违约情形的「不履行」
# 误判成否定表述，故只认「免责表述 × 义务表述」「禁止表述 × 许可表述」
# 这类贴在义务谓语上的组合。
_EXEMPTION_RE = re.compile(r"(不承担|无需|无须|不用|不必|免除|可以不|有权拒绝|拒绝承担)")
_DUTY_RE = re.compile(r"(应当承担|必须承担|承担[^，。；]{0,6}责任|应当赔偿|应当履行"
                      r"|应当[^，。；]{0,32}支付赔偿金)")


def _claim_exempts(claim):
    """论断侧的免责判定（859 收紧）：「无需/免除」类词后面 12 字内
    必须出现责任/赔偿/支付/履行/义务才算免责表述——「无需证明
    损失」是举证话题不是免责，无范围地匹配会误报极性冲突。"""
    for m in _EXEMPTION_RE.finditer(claim or ""):
        if re.search(r"(责任|赔偿|支付|履行|义务)",
                     claim[m.end():m.end() + 12]):
            return True
    return False
_PROHIBIT_RE = re.compile(r"(不得|禁止|不准)")
_PERMIT_RE = re.compile(r"(可以|有权)")
_NUMBER_RE = re.compile(
    r"([0-9]+(?:\.[0-9]+)?|[零一二三四五六七八九十百千]+|半)"
    r"\s*(%|％|元|万元|亿元|日|天|个月|月|年|倍|成|分|条|款|项|人|次|级)?")
# 款项序号（一）（二）…是条文结构编号，不是数量：抽数字前先剔除，
# 否则引文里的「（三）」会给论断中凭空出现的「三倍」提供假依据（859 实测）。
_ENUM_MARK_RE = re.compile(r"[（(][一二三四五六七八九十]+[）)]")
# 「第三人/第一方」是序数称谓不是数量，同理剔除（859 实测：
# 「第三人代为履行」曾被抽出数字「3人」误报数字冲突）。
_ORDINAL_PARTY_RE = re.compile(r"第[一二三四五六七八九十]+(人|方|者)")

# 单位归一化与同族判定（857 v2）：「三个月」冒充「三年」这类同值异单位
# 偷换，v1 只给提示信号；v2 在同族（期限/金额/比例）内视为硬冲突。
# 「天」与「日」、「个月」与「月」是同一单位的不同写法，先归一化。
_UNIT_NORM = {"天": "日", "个月": "月", "％": "%"}
_UNIT_FAMILIES = ({"日", "月", "年"}, {"元", "万元", "亿元"},
                  {"%", "倍", "成", "分"})


def _norm_unit(unit):
    return _UNIT_NORM.get(unit, unit)


def _unit_family(unit):
    for fam in _UNIT_FAMILIES:
        if unit in fam:
            return fam
    return None
_STOP_BIGRAMS = {"的规", "规定", "应当", "可以", "对于", "以及", "或者",
                "但是", "如果", "进行", "相关", "根据", "按照", "依法",
                "本法", "其中", "以上", "以下", "以内", "之日起"}


def _extract_numbers(text):
    out = []
    cleaned = _ORDINAL_PARTY_RE.sub(
        "某方", _ENUM_MARK_RE.sub("", text or ""))
    for m in _NUMBER_RE.finditer(cleaned):
        raw, unit = m.group(1), m.group(2) or ""
        try:
            if raw == "半":
                value = 0.5
            elif raw.replace(".", "").isdigit():
                value = float(raw)
            else:
                value = float(cn_to_int(raw))
        except (ValueError, TypeError):
            continue
        out.append((value, unit))
    return out


# 数量边界方向（859 新增）：「三年以上」与「三年以下」数字全同，
# v2 只比数值会把方向对调判成支持。方向按含本数口径分三档：
# above=以上/超过/不少于/至少；below=以下/不超过/至多/以内（含本数）；
# strict=不满/未满（不含本数）。同一数值＋单位上方向不同档即偷换。
# 方向词位置不固定：「三年以上」在后、「不满三年」「不超过三年」在前，
# 两侧都要看；「不超过」含「超过」，前缀判定优先于后缀。
_DIR_PREFIX = (
    ("strict", re.compile(r"(?:不满|未满)$")),
    ("below", re.compile(r"(?:不超过|至多)$")),
    ("above", re.compile(r"(?:至少|不少于)$")),
)
_DIR_SUFFIX = (
    ("below", re.compile(r"^(?:以下|以内)")),
    ("above", re.compile(r"^(?:以上|超过)")),
)
_DIR_ZH = {"above": "以上向", "below": "以下向（含本数）",
           "strict": "不满向（不含本数）"}


def _extract_quantities(text):
    """抽（数值， 归一单位）→ 方向档集合。"""
    out: dict[tuple, set] = {}
    cleaned = _ORDINAL_PARTY_RE.sub(
        "某方", _ENUM_MARK_RE.sub("", text or ""))
    for m in _NUMBER_RE.finditer(cleaned):
        raw, unit = m.group(1), _norm_unit(m.group(2) or "")
        try:
            if raw == "半":
                value = 0.5
            elif raw.replace(".", "").isdigit():
                value = float(raw)
            else:
                value = float(cn_to_int(raw))
        except (ValueError, TypeError):
            continue
        head = cleaned[max(0, m.start() - 3):m.start()]
        tail = cleaned[m.end():m.end() + 3]
        cls = None
        for name_, pat in _DIR_PREFIX:
            if pat.search(head):
                cls = name_
                break
        if cls is None:
            for name_, pat in _DIR_SUFFIX:
                if pat.search(tail):
                    cls = name_
                    break
        if cls:
            out.setdefault((value, unit), set()).add(cls)
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


# --- 857 v2 新增四类偷换检测（金标集 tests/support_gold.jsonl 量过） ---
# 排他性限定：论断加「只能/唯一/只有…」而引文只是列举或一般规定。
_EXCLUSIVE_CLAIM_WORDS = ("唯一", "只能", "仅限", "唯有", "只有")
_EXCLUSIVE_CITE_WORDS = ("唯一", "仅", "只能", "只有", "仅限", "唯有")
# 引文侧的排他还有「非…不可」句式（859 补）：只认词表会把忠实
# 转述误判成排他偷换。
_EXCLUSIVE_CITE_RE = re.compile(r"非[^，。；]{0,10}不(?:可|得|行)")
# 全称量化：只做降级信号（supports→partial），不单独判不支持。
_UNIVERSAL_CLAIM_WORDS = ("一律", "任何情况下", "无一例外", "全部都")
_UNIVERSAL_CITE_WORDS = ("一律", "均", "一切", "任何", "全部")
# 条件偷换：引文里的强条件标记（「另有规定」类套语太常见、误伤忠实
# 转述，故意不收）。论断保留了条件痕迹时不触发。
# 859 分档：强标记（同意/催告/致使不能实现等具体条件）被丢才算偷换；
# 「除外/除非」是弱标记——长条文几乎必带例外句，单独出现时忠实
# 转述另一款也会被误伤（859 实测三例误报均出于此），只留信号。
_CONDITION_CITE_RES = tuple(re.compile(p) for p in (
    r"未经[^，。；]{0,10}", r"经[^，。；]{1,10}同意", r"书面同意",
    r"协商一致", r"根据[^，。；]{0,8}的请求", r"致使[^，。；]{0,10}不能",
    r"不能实现合同目的", r"合同目的不能实现",
    r"影响[^，。；]{0,10}实现", r"经催告",
    r"在[^，。；]{1,40}的前提下", r"经[^，。；]{1,10}(?:批准|许可)"))
_CONDITION_WEAK_RES = tuple(re.compile(p) for p in (r"除外", r"除非"))
_CONDITION_CLAIM_RES = tuple(re.compile(p) for p in (
    r"根据[^，。；]{0,8}的请求", r"经[^，。；]{1,10}同意"))
_CONDITION_CLAIM_WORDS = ("除外", "除非", "另有", "未经", "协商一致",
                          "书面同意", "致使", "同意", "催告", "如果")
# 论断附加内容：引文外法律后果/程序词，只做降级信号。
_EXTRA_CLAIM_TERMS = ("连带责任", "批准", "备案", "登记", "公告",
                      "仲裁", "抵扣")
# 主体偷换：只认成对主体在同一动作上的归属对调。
_SUBJECT_PAIRS = (("用人单位", "劳动者"), ("用人单位", "工作人员"),
                  ("出租人", "承租人"), ("债权人", "债务人"),
                  ("出卖人", "买受人"), ("经营者", "消费者"),
                  ("贷款人", "借款人"), ("发包人", "承包人"),
                  ("投保人", "保险人"))
_SUBJECT_PAIR_SET = {frozenset(p) for p in _SUBJECT_PAIRS}
_SUBJECTS = sorted({s for pair in _SUBJECT_PAIRS for s in pair},
                   key=len, reverse=True)
_MODAL_RE = re.compile(r"(可以|应当|有权|不得|必须|不能|无需|无须)")
_CONDITION_SPAN_RE = re.compile(r"(?:未经|经)[^，。；]{1,12}同意")
_BEARER_RE = re.compile(
    r"由([^，。；]{2,7}?)(承担|负责|支付|赔偿|履行|返还)")
_ACTION_VERB_RE = re.compile(
    r"(" + "|".join(re.escape(s) for s in (
        "用人单位", "劳动者", "工作人员", "出租人", "承租人", "债权人",
        "债务人", "出卖人", "买受人", "经营者", "消费者", "贷款人",
        "借款人", "发包人", "承包人", "投保人", "保险人"))
    + r")[^，。；、]{0,10}?(解除|终止)")
_PASSIVE_VERB_RE = re.compile(
    r"被(" + "|".join(re.escape(s) for s in (
        "用人单位", "劳动者", "工作人员", "出租人", "承租人", "债权人",
        "债务人", "出卖人", "买受人", "经营者", "消费者", "贷款人",
        "借款人", "发包人", "承包人", "投保人", "保险人"))
    + r")[^，。；、]{0,6}?(解除|终止)")
_OBJECT_PREPS = ("对", "向", "给", "替")


def _actor_bindings(text):
    """抽取「主体—动作」绑定：情态词前最近的主体 → 动作前两字。

    「经X同意/未经X同意」是前置条件而非动作主体，先掩码剔除，
    否则转租类条文会把同意方误当动作方。
    859 修正三处实测误判源：①「对劳动者造成损害」里的劳动者是
    宾语不是动作方，紧邻对/向/给/替之后的主体不作绑定候选；
    ②「应当由X承担」由 BEARER 路径处理，情态路径不再抢先绑出
    「由用」这类伪动作；③补被动与动词直绑：「被劳动者解除」、
    「用人单位解除」把解除/终止的实施方绑到动词上，主被动对调
    才检得出。
    """
    masked = _CONDITION_SPAN_RE.sub("某某", text or "")
    bindings: dict[str, set] = {}
    for m in _MODAL_RE.finditer(masked):
        if masked[m.end():m.end() + 1] == "由":
            continue
        before = masked[max(0, m.start() - 40):m.start()]
        actor, best = None, -1
        for subj in _SUBJECTS:
            pos = before.rfind(subj)
            while pos > 0 and before[pos - 1] in _OBJECT_PREPS:
                pos = before.rfind(subj, 0, pos)
            if pos > best:
                actor, best = subj, pos
        if actor:
            action = re.sub(r"[^一-鿿A-Za-z]", "",
                            masked[m.end():m.end() + 8])[:2]
            if len(action) == 2:
                bindings.setdefault(action, set()).add(actor)
    for m in _BEARER_RE.finditer(masked):
        bearer = m.group(1)
        for subj in _SUBJECTS:
            if subj in bearer:
                bindings.setdefault(m.group(2)[:2], set()).add(subj)
    for m in _ACTION_VERB_RE.finditer(masked):
        bindings.setdefault(m.group(2), set()).add(m.group(1))
    for m in _PASSIVE_VERB_RE.finditer(masked):
        bindings.setdefault(m.group(2), set()).add(m.group(1))
    return bindings


def tool_check_support(args):
    claim = _arg_str(args, "claim", required=True,
                     hint="下一步：请给出待判断的论断原文。").strip()
    if not claim:
        raise SourceError("claim（待判断的论断）不能为空。下一步："
                          "请给出待判断的论断原文。")
    citation_text = _arg_str(args, "citation_text").strip()
    citation_meta = {}
    if not citation_text:
        law_name = _arg_str(args, "law").strip()
        article_in = args.get("article")
        if article_in is not None and (isinstance(article_in, bool)
                                       or not isinstance(article_in,
                                                         (str, int))):
            raise SourceError(
                f"参数 article 类型不对：需要文本或整数，收到的是"
                f"{_type_zh(article_in)}。下一步：请把条号写成如"
                "「第五百七十七条」或 577 再发。")
        if not law_name or article_in is None:
            raise SourceError("需要 citation_text，或 law＋article 指定引注来源。"
                              "下一步：请二选一给出——引注原文，"
                              "或法规名＋条号。")
        out = tool_get_article({"law": law_name, "article": article_in})
        citation_text = out["text"]
        citation_meta = {"law": out["law"]["title"],
                         "article": out["article"],
                         "status": out["law"]["status"],
                         "source_url": out["law"]["source_url"],
                         "text_source": out["text_source"]}
    signals = []
    conflicts = []
    claim_bigrams = _content_bigrams(claim)
    cite_bigrams = _content_bigrams(citation_text)
    coverage = (len(claim_bigrams & cite_bigrams) / len(claim_bigrams)
                if claim_bigrams else 0.0)

    # 信号 1：数字一致性（论断里的具体数字必须在引文中出现）
    claim_nums = [(v, _norm_unit(u)) for v, u in _extract_numbers(claim)]
    cite_nums = [(v, _norm_unit(u)) for v, u in _extract_numbers(citation_text)]
    cite_num_set = set(cite_nums)
    cite_values = {v for v, _u in cite_nums}
    missing_nums = []
    for value, unit in claim_nums:
        if (value, unit) in cite_num_set:
            continue
        if unit and value in cite_values:
            # 同值异单位：同族（期限/金额/比例）内是偷换，判硬冲突；
            # 跨族只留信号请人工核对（v1 口径）。
            fam = _unit_family(unit)
            cite_units = {u for v, u in cite_nums if v == value and u}
            if fam and any(_unit_family(u) == fam for u in cite_units):
                conflicts.append(
                    f"数字单位偷换：论断作「{int(value) if value == int(value) else value}"
                    f"{unit}」，引文中同数值用的是"
                    f"「{'/'.join(sorted(cite_units))}」，期限/金额/比例口径不同")
            else:
                signals.append(f"数字 {value} 在引文中出现但单位不同"
                               f"（论断用「{unit}」），请人工核对")
            continue
        if not unit and value in cite_values:
            continue
        missing_nums.append((value, unit))
    if missing_nums:
        desc = "、".join(f"{int(v) if v == int(v) else v}{u}"
                         for v, u in missing_nums)
        if coverage >= 0.2 or len(missing_nums) >= 2:
            # 判冲突的门槛：词面有基本重合，或论断一次塞进 ≥2 个
            # 引文没有的数字（短论断塞两个编造数字是典型造假形态，
            # 覆盖率会因论断太短而偏低，不能只看覆盖率——859 实测
            # 两套金标各有一例卡在这条边界的两侧，按此复合门槛分开）
            conflicts.append(f"论断中的数字 {desc} 未在引文中出现，"
                             "数字依据可能不存在或张冠李戴")
        else:
            # 论断与引文整体几乎无关、且只有一个数字对不上时，
            # 「数字不在引文中」是弱证据：引文可能根本讲的是另一
            # 回事，只留信号不判冲突（859）。
            signals.append(f"数字 {desc} 未在引文中出现，但论断与引文"
                           "词面重合过低，未判冲突，请人工核对")

    # 信号 1b（859 新增）：数量边界方向偷换——数值全同但以上/以下/
    # 不满方向被改。引文同值上同时有多个方向（如基本档与加重档并存）
    # 时不判，避免误伤。
    claim_q = _extract_quantities(claim)
    cite_q = _extract_quantities(citation_text)
    for key, claim_dirs in claim_q.items():
        cite_dirs = cite_q.get(key)
        if not cite_dirs or claim_dirs & cite_dirs:
            continue
        value, unit = key
        conflicts.append(
            f"数量边界方向偷换：数值 {int(value) if value == int(value) else value}"
            f"{unit} 在引文中是{_DIR_ZH[next(iter(cite_dirs))]}，"
            f"论断却作{_DIR_ZH[next(iter(claim_dirs))]}，"
            "以上/以下/不满的含本数口径不同，范围已被改动")
        break

    # 信号 1f（860 新增）：多档并存时的分档锚定。引文同值多方向
    # （基本档与加重档并存）时 1b 按设计不触发——但论断若明说了
    # 适用情形（如「数额较大」），可按情形把论断锚定到对应分句再
    # 比方向：锚定档与论断方向不符就是「引对条、引错档」。锚定
    # 要求情形词面重合有明确优势（≥4 且领先次选 ≥2），不够明确
    # 不判。论断不提情形、只把后续分档的方向当基本档说的，词面
    # 无法与「只摘述加重档」可靠区分，只留信号请人工核对。
    for key, claim_dirs in claim_q.items():
        cite_dirs = cite_q.get(key)
        if not cite_dirs or len(cite_dirs) < 2:
            continue
        if not (claim_dirs & cite_dirs):
            continue  # 与全部档位都不符的已由 1b 处理
        clauses = [seg for seg in re.split(r"[；;。]",
                                           citation_text or "")
                   if seg.strip()]
        key_clauses = []
        for seg in clauses:
            q = _extract_quantities(seg)
            if key in q:
                key_clauses.append((seg, q[key]))
        if len(key_clauses) < 2:
            continue
        claim_circ = _content_bigrams(re.sub(
            r"[0-9零一二三四五六七八九十百千两半]+"
            r"(?:年|月|日|元|倍|%|％)", "", claim))
        scored = sorted(
            ((len(claim_circ & _content_bigrams(seg)), seg, dirs)
             for seg, dirs in key_clauses),
            key=lambda x: -x[0])
        value, unit = key
        vtxt = int(value) if value == int(value) else value
        best_score, best_seg, best_dirs = scored[0]
        if best_score >= 4 and best_score >= scored[1][0] + 2 \
                and not (claim_dirs & best_dirs):
            conflicts.append(
                f"分档方向偷换：论断所述情形与引文中"
                f"「{best_seg.strip()[:40]}…」一档对应，该档对数值 "
                f"{vtxt}{unit} 的口径是{_DIR_ZH[next(iter(best_dirs))]}，"
                f"论断却作{_DIR_ZH[next(iter(claim_dirs))]}"
                "——引对了条、引错了档")
            break
        first_dirs = key_clauses[0][1]
        if not (claim_dirs & first_dirs):
            for seg, dirs in key_clauses[1:]:
                if not (claim_dirs & dirs):
                    continue
                head = re.split(
                    r"[0-9零一二三四五六七八九十百千两半]", seg)[0]
                absent = _content_bigrams(head) - claim_circ
                if len(absent) >= 2:
                    signals.append(
                        "论断的档位方向与引文中该数值的首档不同、与"
                        "后续分档一致，但论断未提及后续分档的适用"
                        "情形，请人工核对是否把加重档当作基本档、"
                        "或只摘述加重档而未写明其适用情形")
                break

    # 信号 1c（859 新增）：论断断言「无上限」而引文明明设了上限。
    if re.search(r"(?:没有|无|不受)[^，。；]{0,4}(?:上限|限制)", claim) \
            and any(w in citation_text
                    for w in ("不超过", "最高", "上限")):
        conflicts.append("上限偷换：论断称没有上限/不受限制，引文却明确"
                         "设了上限（如「不超过」），结论与引文相反")

    # 信号 1d（859 新增）：论断断言「没有任何例外」而引文明明留了
    # 「法律另有规定的，依照其规定」的例外通道。
    if re.search(r"(?:没有任何|没有|无)[^，。；]{0,4}例外", claim) \
            and "另有规定" in citation_text:
        conflicts.append("例外偷换：论断断言没有任何例外，引文却明确留有"
                         "「法律另有规定的，依照其规定」的例外通道")

    # 信号 1e（859 新增）：论断称可判死刑而引文刑罚里没有死刑。
    if "死刑" in claim and "死刑" not in citation_text \
            and coverage >= 0.2:
        conflicts.append("刑罚种类偷换：论断称可判处死刑，引文规定的"
                         "刑罚中没有死刑")

    # 信号 2：否定极性冲突（论断与引文一否一肯且词面高度重合时才算）
    claim_neg = _has_negation(claim)
    cite_neg = _has_negation(citation_text)
    if _claim_exempts(claim) and _DUTY_RE.search(citation_text) \
            and coverage >= 0.2:
        conflicts.append("极性冲突：论断在免除/否定责任，引文恰恰规定"
                         "应当承担责任（义务方向相反）")
    elif _DUTY_RE.search(claim) and _EXEMPTION_RE.search(citation_text) \
            and coverage >= 0.2:
        conflicts.append("极性冲突：论断主张应当承担责任，引文却含免责表述"
                         "（义务方向相反）")
    elif re.search(r"可以不", claim) and "应当" in citation_text \
            and "可以不" not in citation_text and coverage >= 0.25:
        conflicts.append("极性冲突：论断称可以不履行，引文却规定应当履行"
                         "（义务方向相反）")
    elif _PROHIBIT_RE.search(citation_text) and _PERMIT_RE.search(claim) \
            and not claim_neg and coverage >= 0.25:
        conflicts.append("极性疑似冲突：引文含禁止表述（不得/禁止/不准），"
                         "论断却表述为可以/有权")
    elif _PROHIBIT_RE.search(claim) and coverage >= 0.25 and any(
            not re.search(r"(不|未|没有|无)$",
                          citation_text[max(0, pm.start() - 4):pm.start()])
            and any(a in claim and a in citation_text
                    for a in ("请求", "返还", "解除", "终止", "追偿",
                              "主张", "赔偿"))
            for pm in _PERMIT_RE.finditer(citation_text)):
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

    # 信号 3b（859 新增）：组织形态错位——「股份有限公司」与
    # 「有限责任公司」只差两字、词面覆盖极高，v2 会把讲错公司
    # 形态的论断判成支持；两形态互斥，claim/cite 各执一词即冲突。
    for form_a, form_b in (("股份有限公司", "有限责任公司"),):
        if (form_a in claim and form_b in citation_text
                and form_a not in citation_text) or \
                (form_b in claim and form_a in citation_text
                 and form_b not in citation_text):
            conflicts.append(f"组织形态错位：论断讲的是「{form_a if form_a in claim else form_b}」，"
                             f"引文讲的是「{form_b if form_a in claim else form_a}」，"
                             "两种公司形态规则不同，不能混用")
            break

    # 信号 4（857 v2）：排他性限定偷换
    downgrades = []
    if coverage >= 0.2 \
            and any(w in claim for w in _EXCLUSIVE_CLAIM_WORDS) \
            and not any(w in citation_text for w in _EXCLUSIVE_CITE_WORDS) \
            and not _EXCLUSIVE_CITE_RE.search(citation_text):
        conflicts.append("排他性限定偷换：论断附加了「只能/唯一/只有」等"
                         "排他表述，引文并无此限定（引文是列举或一般"
                         "规定），论断把范围收窄到了引文之外")
    elif any(w in claim for w in _UNIVERSAL_CLAIM_WORDS) \
            and not any(w in citation_text for w in _UNIVERSAL_CITE_WORDS):
        downgrades.append("论断用了「一律/任何情况下」等全称表述，"
                          "引文未见同等全称限定，请人工核对范围")

    # 信号 5（857 v2；859 改计数制）：条件偷换——引文设了条件，
    # 论断按无条件表述。859 起按「被丢的强标记数」判：丢 ≥2 个强
    # 条件必判冲突；只丢 1 个且论断毫无条件痕迹时才判；弱标记
    # （除外/除非）单独出现只留信号（长条文的例外句误伤面太大）。
    cite_conds = [p for p in _CONDITION_CITE_RES
                  if p.search(citation_text)]
    claim_keeps_condition = (
        any(w in claim for w in _CONDITION_CLAIM_WORDS)
        or any(p.search(claim) for p in _CONDITION_CLAIM_RES))
    missing_conds = [p for p in cite_conds if not p.search(claim)]
    weak_dropped = any(p.search(citation_text)
                       for p in _CONDITION_WEAK_RES) \
        and not any(w in claim for w in ("除外", "除非"))
    if cite_conds and coverage >= 0.2 \
            and (len(missing_conds) >= 2
                 or (missing_conds and not claim_keeps_condition)):
        conflicts.append("条件偷换：引文设有条件"
                         f"（如「{missing_conds[0].pattern}」等 "
                         f"{len(missing_conds)} 处），论断未保留这些条件、"
                         "按无条件结论表述，适用范围已被改动")
    elif cite_conds and missing_conds:
        signals.append("引文设有条件而论断未全保留，但词面重合过低或"
                       "论断留有条件痕迹，未判冲突，请人工核对")
    if weak_dropped and not cite_conds:
        signals.append("引文带有「除外/除非」例外表述而论断未提及，"
                       "请人工核对例外是否影响论断")

    # 信号 6（857 v2）：主体偷换——成对主体在同一动作上的归属对调
    if coverage >= 0.2:
        claim_bind = _actor_bindings(claim)
        cite_bind = _actor_bindings(citation_text)
        for action, claim_actors in claim_bind.items():
            cite_actors = cite_bind.get(action)
            if not cite_actors or claim_actors & cite_actors:
                continue
            swapped = [(a, b) for a in claim_actors for b in cite_actors
                       if frozenset((a, b)) in _SUBJECT_PAIR_SET]
            if swapped:
                a, b = swapped[0]
                conflicts.append(f"主体偷换：动作「{action}…」在引文中归"
                                 f"「{b}」，论断却归「{a}」，成对主体被对调")
                break

    # 信号 7（857 v2，只降级）：论断附加引文没有的程序/后果内容
    extras = [t for t in _EXTRA_CLAIM_TERMS
              if t in claim and t not in citation_text]
    if extras:
        downgrades.append(f"论断附加了引文没有的内容（{'、'.join(extras)}），"
                          "该部分无引文依据，请人工核对")
    signals.extend(downgrades)

    # 排他对齐（859）：论断用「只能」类排他表述、引文本身也有排他
    # 标记（含「非…不可」句式）时，两侧的排他语义是对齐的——这类
    # 忠实转述常因措辞不同词面覆盖偏低，覆盖 ≥0.4 且无其他降级
    # 信号时按支持处理，不再一律压成「部分支持」。
    exclusive_aligned = (
        any(w in claim for w in _EXCLUSIVE_CLAIM_WORDS)
        and (any(w in citation_text for w in _EXCLUSIVE_CITE_WORDS)
             or _EXCLUSIVE_CITE_RE.search(citation_text)))
    if conflicts:
        verdict = "unsupported"
        verdict_zh = "初筛：不支持（检出硬冲突信号，见 reasons）"
    elif exclusive_aligned and coverage >= 0.4 and not downgrades:
        verdict = "supports"
        verdict_zh = ("初筛：支持（论断与引文的排他性表述两侧对齐，"
                      "词面部分重合）")
    elif coverage >= 0.5 and downgrades:
        verdict = "partial"
        verdict_zh = ("初筛：部分支持（主体内容有词面依据，但有附加/全称"
                      "表述超出引文范围，见 reasons）")
    elif coverage >= 0.5:
        verdict = "supports"
        verdict_zh = "初筛：支持（词面高度重合且无冲突信号）"
    elif coverage >= 0.28:
        verdict = "partial"
        verdict_zh = "初筛：部分支持（词面部分重合，可能只覆盖论断的一部分）"
    else:
        verdict = "cannot_determine"
        verdict_zh = "初筛：无法判断（词面重合过低，启发式无从判定）"
    if verdict == "cannot_determine" and downgrades and coverage >= 0.2:
        # 已检出具体的超范围信号（全称/附加内容）时，不许只说
        # 「无法判断」——降为部分支持并把信号亮出来（859）。
        verdict = "partial"
        verdict_zh = ("初筛：部分支持（词面重合有限，且有表述超出"
                      "引文范围，见 reasons）")
    return {
        "schema": RESULT_SCHEMA, "tool": "check_support",
        "method": "lexical_heuristic_v3.1",
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
        "description": "在官方源实时检索法规（三库分工：国家法律法规数据库 flk、"
                       "司法部行政法规库 moj、国家规章库 gzk），返回带效力状态、"
                       "官方链接与所属库（library）标记的法规列表。引用任何法条前"
                       "先用它定位法规。gzk 只提供元数据与摘要、无官方效力字段，"
                       "其结果 status 恒为「效力未核」。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索词（法名或关键词）"},
                "mode": {"type": "string", "enum": ["exact", "fuzzy"],
                         "description": "exact=精确短语，fuzzy=模糊（默认）"},
                "scope": {"type": "string", "enum": ["title", "body"],
                          "description": "title=只查标题（默认），body=查正文"},
                "limit": {"type": "integer", "description": "每库返回条数，1–20，默认 10"},
                "source": {"type": "string",
                           "enum": ["auto", "flk", "moj", "gzk"],
                           "description": "auto=三库合并（默认）；flk=国家法律法规"
                                          "数据库；moj=司法部行政法规库；"
                                          "gzk=国家规章库（部门/地方政府规章）"},
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
                       "法规已废止/尚未生效、引文与原文不逐字一致都会被明确指出。"
                       "核验顺序：本地快照/国家法律法规数据库（flk）→司法部行政"
                       "法规库（moj）→国家规章库（gzk，仅存在性、效力未核），"
                       "结论恒标注通道与数据来源。",
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
                       "否定极性冲突、排他性限定、条件偷换、单位偷换、主体偷换），"
                       "输出 supports/partial/unsupported/cannot_determine 四档"
                       "＋逐条理由。重要：这是初筛，不能替代人工判断；"
                       "每次输出都带此口径说明。",
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
                                 "text": f"未知工具：{name}。下一步：请先用 "
                                         "tools/list 查看可用工具名。"}],
                    "isError": True}
        call_args = params.get("arguments") or {}
        if not isinstance(call_args, dict):
            return {"content": [{"type": "text",
                                 "text": "参数 arguments 类型不对：需要"
                                         "对象（键值对），收到的是"
                                         f"{_type_zh(call_args)}。下一步："
                                         "请按工具的参数说明，以对象形式"
                                         "传参。"}],
                    "isError": True}
        try:
            result = tool["_impl"](call_args)
            return {"content": [{"type": "text",
                                 "text": json.dumps(result, ensure_ascii=False,
                                                    indent=2)}],
                    "isError": False}
        except (SourceError, ValueError) as e:
            return {"content": [{"type": "text", "text": f"错误：{e}"}],
                    "isError": True}
        except Exception:  # 兜底：任何未预料异常只许变工具错误，绝不许打崩进程
            log("工具调用未捕获异常：\n" + traceback.format_exc())
            return {"content": [{"type": "text",
                                 "text": "错误：工具内部出现未预料的问题，"
                                         "本次调用已终止，服务进程未受影响。"
                                         "下一步：请核对参数后重试；若反复"
                                         "出现，请把本次参数提供给维护者。"}],
                    "isError": True}
    if mid is None:
        return None  # 通知，无需响应
    return {"_error": {"code": -32601, "message": f"Method not found: {method}"}}


def serve_stdio():
    log(f"{SERVER_NAME} v{SERVER_VERSION} stdio 就绪")
    out = sys.stdout

    def _emit(resp):
        out.write(json.dumps(resp, ensure_ascii=False) + "\n")
        out.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as e:
            # 与 HTTP 传输对齐（JSON-RPC 规范）：坏 JSON 回 -32700，
            # 不许静默吞掉，也不许因此退出进程
            _emit({"jsonrpc": "2.0", "id": None,
                   "error": {"code": -32700,
                             "message": f"Parse error：收到的不是合法 "
                                        f"JSON（{e.msg}）"}})
            continue
        try:
            if isinstance(msg, list):
                # 批量请求与 HTTP 传输同口径处理（859 对齐）：逐条
                # 处理、非对象条目回 -32600、通知不回，整体以数组
                # 一次返回；857 版把整批拒掉，与 HTTP 行为不一致。
                # 例外：整批没有任何合法条目（含空数组）时按
                # JSON-RPC 规范回单个 -32600（857 既有口径，保留）。
                if not any(isinstance(e, dict) for e in msg):
                    _emit({"jsonrpc": "2.0", "id": None,
                           "error": {"code": -32600,
                                     "message": "Invalid Request："
                                                "批量请求不含任何合法条目"}})
                    continue
                responses = []
                for entry in msg:
                    if not isinstance(entry, dict):
                        responses.append(
                            {"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32600,
                                       "message": "Invalid Request："
                                                  "批量条目须为 JSON 对象"}})
                        continue
                    r = handle_request(entry)
                    if entry.get("id") is not None and r is not None:
                        responses.append(_jsonrpc_response(entry, r))
                if responses:
                    _emit(responses)
                continue
            if not isinstance(msg, dict):
                _emit({"jsonrpc": "2.0", "id": None,
                       "error": {"code": -32600,
                                 "message": "Invalid Request：请求须为 "
                                            "JSON 对象"}})
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
                resp = {"jsonrpc": "2.0", "id": msg.get("id"),
                        "result": result}
            _emit(resp)
        except Exception:  # 单条消息处理失败绝不许带崩整个进程
            log("stdio 消息处理未捕获异常：\n" + traceback.format_exc())
            _emit({"jsonrpc": "2.0",
                   "id": msg.get("id") if isinstance(msg, dict) else None,
                   "error": {"code": -32603,
                             "message": "Internal error：本条消息处理失败，"
                                        "服务进程未受影响，请核对后重试"}})


# ---------------------------------------------------------------------------
# Streamable HTTP 传输（与 stdio 共用 handle_request 同一核验内核）
# ---------------------------------------------------------------------------

def _jsonrpc_response(msg, result):
    if isinstance(result, dict) and "_error" in result:
        return {"jsonrpc": "2.0", "id": msg.get("id"), "error": result["_error"]}
    return {"jsonrpc": "2.0", "id": msg.get("id"), "result": result}


_HTTP_MAX_BODY = 10 * 1024 * 1024


def serve_http(host, port):
    """MCP Streamable HTTP：POST /mcp 收 JSON-RPC（单条或批量），

    请求回 application/json，通知回 202；GET 不提供服务端推送，回 405。
    无状态实现：不要求 Mcp-Session-Id（与本 server 的无状态请求处理一致）。
    默认只绑 127.0.0.1；对外托管时请置于反向代理之后并自行加访问控制。
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        # 860 修复：连接读超时。旧版无任何超时——客户端声明一个
        # 未超上限的 Content-Length 后只发半截、保持连接不发完，
        # handler 线程就无限期挂在 rfile.read 上（一连接钉死一
        # 线程，实测保持 40 秒纹丝不动、只能等客户端自己断开）。
        # 30 秒读不到新数据即断开，慢连接占用有上限。
        timeout = 30

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
            if length > _HTTP_MAX_BODY:
                # 不读超大请求体（防内存耗尽），直接拒（859）；
                # 未读的请求体还留在连接上，保活复用会把残余字节
                # 当成下一条请求，拒后必须关连接（860）
                self.close_connection = True
                self._send(413, {"error": "请求体超过上限"
                                          f"（{_HTTP_MAX_BODY} 字节）"})
                return
            raw = self.rfile.read(length) if length > 0 else b""
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self._send(400, {"jsonrpc": "2.0", "id": None,
                                 "error": {"code": -32700,
                                           "message": "Parse error"}})
                return
            if isinstance(payload, list) and not any(
                    isinstance(e, dict) for e in payload):
                # 与 stdio 同口径：整批无合法条目（含空数组）回
                # 单个 -32600（JSON-RPC 规范口径）
                self._send(200, {"jsonrpc": "2.0", "id": None,
                                 "error": {"code": -32600,
                                           "message": "Invalid Request："
                                                      "批量请求不含任何"
                                                      "合法条目"}})
                return
            messages = payload if isinstance(payload, list) else [payload]
            responses = []
            for msg in messages:
                if not isinstance(msg, dict):
                    # 与 stdio 同口径：非对象条目回 -32600，不许
                    # 静默吞掉（859 对齐，857 版直接跳过）
                    responses.append(
                        {"jsonrpc": "2.0", "id": None,
                         "error": {"code": -32600,
                                   "message": "Invalid Request：批量条目"
                                              "须为 JSON 对象"}})
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

    ThreadingHTTPServer.daemon_threads = True
    server = ThreadingHTTPServer((host, port), Handler)
    log(f"{SERVER_NAME} v{SERVER_VERSION} Streamable HTTP 就绪："
        f"http://{host}:{port}/mcp")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def server_health():
    """健康检查（857）：快照概况＋三库实时通道轻量探活。

    探活只发最小检索请求，不下载全文；某一路失败如实标 down 并附
    人话原因，绝不把失败涂成正常。供 --health CLI 与运维巡检用。
    """
    health = {"server": SERVER_NAME, "version": SERVER_VERSION,
              "checked_at": datetime.now(timezone.utc).isoformat()}
    info = snapshot_info()
    if info is None:
        health["snapshot"] = {"available": False,
                              "detail": "本地快照库不存在或不可读，"
                                        "快照通道不可用"}
    else:
        health["snapshot"] = {
            "available": True, "law_count": info["law_count"],
            "article_count": info["article_count"],
            "built_at": info["built_at"],
        }
    probes = {}

    def _probe(label, fn):
        started = time.time()
        try:
            detail = fn()
            probes[label] = {"ok": True, "detail": detail,
                             "elapsed_ms": int((time.time() - started) * 1000)}
        except SourceError as e:
            probes[label] = {"ok": False, "detail": str(e),
                             "elapsed_ms": int((time.time() - started) * 1000)}
        except Exception as e:  # 探活自身不许抛错打崩调用方
            probes[label] = {"ok": False,
                             "detail": f"探活异常：{type(e).__name__}",
                             "elapsed_ms": int((time.time() - started) * 1000)}

    _probe("flk", lambda: f"国家法律法规数据库可达"
                          f"（探活检索返回 {_flk_probe_count()} 条）")
    _probe("moj", _moj_probe)
    _probe("gzk", lambda: f"国家规章库可达"
                          f"（探活检索共 {gzk_search_rows('管理办法', limit=1)[0]} 条）")
    health["live_channels"] = probes
    health["ok"] = health["snapshot"]["available"] or \
        any(p["ok"] for p in probes.values())
    return health


def _flk_probe_count():
    _total, rows = flk_search_rows("民法典", search_type=1, page_size=1)
    return len(rows)


def _moj_probe():
    """moj 探活（859 改口径）：详情可达只是其一，还要报标题检索的
    实际能力——服务端过滤失效是已知事实，探活若只报「可达」会把
    检索层面的不可用涂成全绿。判定：详情可达＋索引在位＝该通道
    实际可用；索引缺失时在 detail 里明说覆盖极有限。
    """
    detail = moj_fetch_detail("7")
    base = f"司法部行政法规库详情通道可达（{detail['title'][:20]}…）"
    idx = _moj_index()
    if idx["items"]:
        base += (f"；标题索引在位（{len(idx['items'])} 部，构建于 "
                 f"{idx['built_at']}），标题检索覆盖全库")
        stale = _moj_index_stale_note()
        if stale:
            base += "；" + stale
    else:
        base += ("；标题索引缺失：moj 标题检索只能命中服务端返回的"
                 "第一页（覆盖极有限），请先跑 tools/build_moj_index.py")
    try:
        url = (f"{MOJ_BASE}/SearchTitleFront?" + urllib.parse.urlencode(
            {"SiteID": "122", "Query": "ZZZ不存在的法规QQQ859",
             "Type": "1"}))
        html_text = _fetch_alt_text(url, "司法部行政法规库检索", MOJ_BASE)
        raw_items = _moj_parse_list(html_text)
        base += "；服务端检索过滤：" + (
            "已生效" if not raw_items else
            "未生效（已知限制，标题检索由本地索引接管）" if idx["items"]
            else "未生效（已知限制，且无本地索引兜底）")
    except SourceError:
        base += "；服务端检索过滤探活未完成"
    return base


def main():
    argv = sys.argv[1:]
    if "--version" in argv:
        print(f"{SERVER_NAME} {SERVER_VERSION}")
        return 0
    if "--health" in argv:
        health = server_health()
        print(json.dumps(health, ensure_ascii=False, indent=2))
        return 0 if health["ok"] else 1
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
