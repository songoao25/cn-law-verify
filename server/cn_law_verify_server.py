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
SERVER_VERSION = "0.5.3"
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
        # 对照表式文件兜底（867）：罪名补充规定类文件的条号键独立
        # 成段、标准标题式截取全灭时，按表格结构截取。
        for seg in _table_article_segments(paragraphs):
            if seg["num"] == num and seg["sub"] == sub and seg["text"]:
                return {"heading": target, "text": seg["text"]}
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
    result = [best[k] for k in order]
    if not result:
        # 标准切条全灭才启用对照表兜底（867，见 _table_article_segments）
        result = [seg for seg in _table_article_segments(paragraphs)
                  if seg["text"]]
    return result


_TABLE_KEY_RE = re.compile(
    r"^第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?"
    r"(?:\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*款)?$")


def _table_article_segments(paragraphs):
    """对照表式法规（如罪名补充规定）的切条兜底（867 新增）。

    这类文件正文是表格拍平的段落：条号键（「第344条」「第345条
    第3款」「第244条之一」）独立成段，其后段落是该条内容（来源
    标注＋罪名等），直到下一个键段。键带「第K款」时条文文本以
    「第K款」起头——与官方罪名表「刑法条文」列的呈现及 865 真值
    档案的切条口径一致。键段不足 2 个（疑似偶然命中）返回空表，
    不让普通法规误入本兜底。
    """
    keys = []
    for i, para in enumerate(paragraphs):
        p = _fw_norm((para or "").strip()).replace("條", "条")
        if not p or len(p) > 24:
            continue
        m = _TABLE_KEY_RE.match(p)
        if not m:
            continue
        try:
            num = cn_to_int(m.group(1))
            sub = cn_to_int(m.group(2)) if m.group(2) else None
            kuan = cn_to_int(m.group(3)) if m.group(3) else None
        except ValueError:
            continue
        keys.append((i, num, sub, kuan))
    if len(keys) < 2:
        return []
    out = []
    for j, (i, num, sub, kuan) in enumerate(keys):
        end = keys[j + 1][0] if j + 1 < len(keys) else len(paragraphs)
        parts = []
        if kuan:
            parts.append(f"第{int_to_cn(kuan)}款")
        parts.extend(p.strip() for p in paragraphs[i + 1:end]
                     if p and p.strip())
        out.append({"num": num, "sub": sub,
                    "heading": article_heading(num, sub),
                    "text": "\n".join(parts).strip()})
    return out


_CITATION_RE = re.compile(
    r"《([^》]{2,60})》\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")

# 867：_CITATION_RE 的 [^》] 在嵌套书名号处即断——《云南省实施
# 《中华人民共和国农村土地承包法》办法》这类法名（含内层《》）
# 整条抽不出，865 paras 16 条未抽取中 15 条属此形态，另 1 条是
# 法名超 60 字上限。extract_citations 改用按嵌套深度扫描的
# _scan_book_title（见下），本式保留仅作历史参照，不再参与抽取。
_ARTICLE_AFTER_TITLE_RE = re.compile(
    r"\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")


def _scan_book_title(src, start):
    """从 src[start]（须为「《」）起按书名号嵌套深度扫描 →
    (法名, 闭合》的下一位下标)；无法闭合返回 (None, start)。"""
    depth = 0
    i = start
    while i < len(src):
        ch = src[i]
        if ch == "《":
            depth += 1
        elif ch == "》":
            depth -= 1
            if depth == 0:
                return src[start + 1:i], i + 1
        i += 1
    return None, start

# 无书名号引注的识别式：859 曾用它把「根据民法典第五百七十七条」
# 这类裸引注也抽出来核验；866（0.5.2，用户裁定）起整段核验只认
# 《》形式，本式不再参与引注抽取，仅保留给 _extract_preceding_quote
# 做防御性判断（引文内部若含引注形态的文字，就不把该段引文关联给
# 后面的引注）。它不会让任何裸引注被抽出或被核验。
_BARE_NAME_FORBIDDEN = "根据依据按照依照关于对于向被将把与及或的了是在由从到给让这那其之说，。；、：:（）()《》「」『』\"'“”‘’\\s"
_BARE_CITATION_RE = re.compile(
    rf"([^{_BARE_NAME_FORBIDDEN}]{{1,30}}?法典"
    rf"|[^{_BARE_NAME_FORBIDDEN}]{{2,30}}?"
    r"(?:条例|规定|办法|细则|解释|规则|章程|通则|法))"
    r"\s*第\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")


def extract_citations(text):
    """从整段文本中抽取引注：只抽《法名》第N条形式（866 定版）。

    不带《》书名号的裸引注（如「根据民法典第五百七十七条」）一律
    不抽取——这是明示的产品边界，见 README「已知盲区」第 9 条。
    在全角数字/繁体「條」归一化后的副本上抽取（逐字 1:1 替换，
    span 与原文位置严格一致）；繁体法名本身不做简繁转换，抽得出、
    定位不到时由核验环节如实报「未检索到」，不静默漏检。
    """
    src = _fw_norm(text or "").replace("條", "条")
    out = []
    i = 0
    while True:
        j = src.find("《", i)
        if j < 0:
            break
        name, end = _scan_book_title(src, j)
        if name is None or not (2 <= len(name) <= 120):
            i = j + 1
            continue
        m = _ARTICLE_AFTER_TITLE_RE.match(src, end)
        if not m:
            i = end
            continue
        try:
            num = cn_to_int(m.group(1))
            sub = cn_to_int(m.group(2)) if m.group(2) else None
        except ValueError:
            i = end
            continue
        out.append({"law": name, "article_num": num,
                    "article_sub": sub,
                    "article": article_heading(num, sub),
                    "span": [j, m.end()]})
        i = m.end()
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


_ORDINAND_RE = re.compile(
    r"第([0-9]+|[零一二三四五六七八九十百千]+)(条|款|项|章|节|编)")


def _normalize_for_compare(text):
    """逐字比对前的归一化：去空白＋序数词写法统一。

    867：官方 DOCX 原文作「第三款」，而引文常作「第3款」（865 真值
    档案的罪名表条文即阿拉伯式）——同一款号两种写法，旧实现只去
    空白，逐字比对在首字即判不一致（true-154 实测 similarity
    0.9773 仍判失败）。第N条/款/项/章/节/编的 N 统一成阿拉伯数字
    后再比；条文正文里的其他数字不动。
    """
    t = re.sub(r"\s+", "", (text or "")).replace("　", "")

    def _canon(m):
        raw = m.group(1)
        if raw.isdigit():
            return f"第{int(raw)}{m.group(2)}"
        try:
            return f"第{cn_to_int(raw)}{m.group(2)}"
        except ValueError:
            return m.group(0)

    return _ORDINAND_RE.sub(_canon, t)


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

    全文已成功取回时，条号在全文中不存在就是不存在——直接返回
    None，不再拿上下文窗口兜底（867 修复）：窗口是全文检索的命中
    片段拼接，里面偶现的同字面条号（如他条引用、目录残片）曾被
    截成「该条原文」，使不存在的条号被误报 article.ok（865 drift
    两题实测）。窗口兜底只保留给全文获取失败（SourceError）的情形。
    """
    notes = []
    heading = article_heading(num, sub)
    try:
        paragraphs = _docx_paragraphs_cached(law["id"], cache)
    except SourceError as e:
        notes.append(f"官方全文（DOCX）获取失败，转入上下文窗口兜底：{e}")
        paragraphs = None
    if paragraphs is not None:
        found = extract_article(paragraphs, num, sub)
        if found:
            return found["text"], "official_docx_full_text", notes
        notes.append("官方全文已取回，其中无此条（条号不存在或已漂移）")
        return None, None, notes
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


def _match_older_version(law, num, sub, citation_text, _excerpt_ok, cache):
    """引文与现行版比对失败时，枚举同名旧版本逐条比对（867 F2）。

    865 repealed 9 题失败的根因：题目引的是已废止旧版条文，而
    find_law 按状态偏好总先定位现行版，拿现行版比对自然对不上，
    状态检查又显示现行版的「有效」，整题被判失败——法规本身的
    历史版本明明在库里。候选来源：同名搜索行＋详情页 lsyg 沿革，
    只比对已废止（sxx=1）与已修改（sxx=2）的旧版，上限 4 部。
    命中 → {"law", "official_text", "text_source"}；否则 None。
    """
    given = _normalize_for_compare(citation_text)
    if not given:
        return None
    cands = {}
    try:
        _, rows = flk_search_rows(law["title"], search_type=1,
                                  search_range=1, page_size=10)
        for r in rows:
            res = row_to_result(r)
            if res["title"] == law["title"] and res["id"] != law["id"]:
                cands[res["id"]] = res
    except Exception:
        pass
    if len(cands) < 4:
        try:
            details = fetch_law_details(law["id"])
            for v in details.get("lsyg") or []:
                vid = v.get("bbbs") if isinstance(v, dict) else None
                if not vid or vid in cands or vid == law["id"]:
                    continue
                d2 = fetch_law_details(vid)
                st = map_status(d2.get("sxx"))
                cands[vid] = {
                    "id": vid, "title": clean_title(d2.get("title")),
                    "authority": d2.get("zdjgName") or "",
                    "category": d2.get("flxz") or "",
                    "promulgated": d2.get("gbrq") or "",
                    "effective_from": d2.get("sxrq") or "",
                    "status": st[0], "status_en": st[1], "status_code": st[2],
                    "source_url": "https://flk.npc.gov.cn/detail.html"
                                  f"?bbbs={vid}",
                }
                if len(cands) >= 4:
                    break
        except Exception:
            pass
    ordered = sorted(
        cands.values(),
        key=lambda c: (0 if c["status_code"] == 1
                       else 1 if c["status_code"] == 2 else 2))
    for cand in ordered:
        if cand["status_code"] not in (1, 2):
            continue
        try:
            text, source, _n = fetch_official_article(cand, num, sub,
                                                      cache=cache)
        except Exception:
            continue
        if text is None:
            continue
        official = _normalize_for_compare(text)
        exact = given == official or (_excerpt_ok and given in official) \
            or (source == "official_context_window"
                and (given in official or official in given))
        if exact:
            return {"law": cand, "official_text": text,
                    "text_source": source}
    return None


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

    # 867 F2 版本感知：带引文、定位版比对已失败（条号未找到或文本
    # 不逐字一致）且定位到的版本本身不是已废止/失效时，枚举同名
    # 旧版本比对——引文可能出自已废止/已修改的旧文本，结论必须
    # 按旧版口径给，不能拿定位版的效力状态冒充整部法的状态。
    # 867 回归补漏：触发不限现行版（sxx=3）——有的法已无现行版，
    # find_law 定位到的就是已修改版（sxx=2，如《甘肃省石油勘探
    # 开发生态环境保护条例》：仅存 2 版已修改＋1 版已废止），
    # 引文出自已废止版时旧触发永不命中（repealed-020/088/156）。
    if (citation_text and article_result is not None
            and law["status_code"] in (2, 3)
            and (not article_result.get("ok")
                 or (checks.get("text") is not None
                     and not checks["text"].get("exact_match")))):
        vm = _match_older_version(law, num, sub, citation_text,
                                  _excerpt_ok, _cache)
        if vm is not None:
            old = vm["law"]
            checks["version_match"] = {
                "matched_version_status": old["status"],
                "matched_version_status_code": old["status_code"],
                "matched_version_id": old["id"],
                "matched_version_promulgated": old.get("promulgated") or "",
                "current_version_id": law["id"],
            }
            if old["status_code"] == 1:
                checks["status"] = {
                    "ok": False, "warning": False,
                    "status": old["status"],
                    "detail": "引文与该法已废止的旧版本逐字一致；该旧"
                              "版本已废止，不得作为现行有效依据引用"
                              + (f"（现行有效版本公布于"
                                 f"{law.get('promulgated') or '—'}）"
                                 if law["status_code"] == 3 else
                                 f"（本次定位到的版本状态为"
                                 f"「{law['status']}」）"),
                }
                checks["article"] = {
                    "article": heading, "ok": True,
                    "official_text": vm["official_text"],
                    "text_source": vm["text_source"],
                    "detail": f"已在已废止旧版本中取回{heading}官方原文",
                }
                checks["text"] = {
                    "ok": True, "exact_match": True,
                    "matched_version": "repealed",
                    "compared_against": vm["text_source"],
                    "excerpt_mode": bool(_excerpt_ok),
                    "detail": "引文与已废止旧版本逐字一致",
                }
                notes.append(
                    f"引文出自《{old['title']}》已废止的旧版本"
                    f"（{old.get('promulgated') or '公布日期不详'}公布），"
                    "与该旧版本逐字一致；引用已废止文本不能作为现行"
                    "依据，请改引现行有效版本对应条文。")
                law = old
                article_result = checks["article"]
            else:
                notes.append(
                    f"引文与《{old['title']}》修改前文本逐字一致；该法"
                    "已经修改，现行文本与引文不一致，引用须以现行"
                    "文本为准。")

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


def _title_extra_suffix(name, title):
    """定位到的法规名＝所给法名（可带国名前缀）＋实质后缀时返回后缀。

    与 _name_mismatch_rest 方向相反：所给《矿产资源法》、库里只有
    《矿产资源法实施条例》——包含命中中的其实是另一部衍生法规，
    865 实测 lookup 因此被吸到实施条例上。仅当前缀差异（《民法典》
    ↔《中华人民共和国民法典》）不算后缀，返回 None。
    """
    if not title or not name:
        return None
    t2 = title[len("中华人民共和国"):] \
        if title.startswith("中华人民共和国") else title
    if t2 == name:
        return None
    if t2.startswith(name):
        rest = t2[len(name):].strip("（）()·、，,：:；;【】[]《》 ")
        if rest and any(k in rest for k in _NAME_EXT_KEYWORDS):
            return rest
    return None


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
    if exact:
        rows.sort(key=lambda r: _STATUS_PREFERENCE.get(r["status_code"], 9))
    else:
        # 867：非精确命中时，带实质后缀的衍生法规（如所问本法只有
        # 其《…实施条例》在库）排到最后——它不是所问的那部法。
        rows.sort(key=lambda r: (
            1 if _title_extra_suffix(name, r["title"]) else 0,
            _STATUS_PREFERENCE.get(r["status_code"], 9),
            abs(len(r["title"]) - len(name))))
    chosen = _snapshot_law_dict(rows[0])
    if not exact:
        rest = _name_mismatch_rest(name, chosen["title"])
        if rest:
            chosen["name_mismatch"] = rest
        extra = _title_extra_suffix(name, chosen["title"])
        if extra:
            chosen["title_extra"] = extra
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
    if snap_law is not None and law_name and (
            snap_law.get("title_extra")
            or (snap_law.get("name_mismatch") and "《" in law_name)):
        # 867：快照命中的是衍生法规（问本法、库里只有其实施条例）；
        # 先让实时通道找本法，找得到就不用快照那一部。
        # 867 回归补漏：复合名（「云南省实施《…》办法」一类含内嵌
        # 书名号）时快照 contains 命中的是内层母法（name_mismatch
        # 为「云南省实施《》办法」），那是另一部法——本法在 flk
        # 有精确条目，必须先让实时通道按全名找本法（865 lookup
        # -051/076/082/136 四题栽在此短路）。
        try:
            live_law, live_match = find_law(law_name)
        except Exception:
            live_law, live_match = None, None
        if live_law is not None and (
                live_match == "exact"
                or not _title_extra_suffix(law_name, live_law["title"])):
            notes.append(
                f"快照库未收录《{law_name}》，仅有其衍生法规"
                f"《{snap_law['title']}》，已转实时通道定位本法")
            snap_law = None
    if snap_law is not None:
        art = snapshot_get_article(snap_law["id"], num, sub)
        if art is not None:
            info = snapshot_info()
            snap_notes = ["原文取自本地快照库（官方 DOCX 全文切条），"
                          "效力状态以快照构建时官方标注为准"]
            if snap_law.get("title_extra"):
                snap_notes.append(
                    f"法名不完全一致：快照库未收录所问法规本身，以下"
                    f"原文属于其衍生法规《{snap_law['title']}》（法规名"
                    f"多出「{snap_law['title_extra']}」），不是所问法规"
                    "本身的条文，引用前请先确认法规无误。")
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
# lookup 专用：法名近名纠错（843b 新增）
#
# 背景：842 评测（开发集）中 lookup 错别字子类 0/20——法名一字之差
# （多为拼音输入的同音错字，如「民法点」「销法」）即三路解析全灭、
# 直接报错，无任何容错。本节给 lookup 加一层确定性近名纠错：仅当
# 快照／flk 实时／moj 三路既有解析全部落空时才启用，在已知法名
# 全集上按编辑距离找候选，阈值从严（≤4 字距离 ≤1、更长 ≤2），
# 必须唯一胜出才自动纠正并在结果里显式标注；编辑距离并列时再按
# 拼音距离分胜负（同音错字是这类输入的主流形态），仍分不出就
# 返回候选清单让人挑，绝不擅自选一个。不用任何语义/LLM 猜测。
#
# 立场：纠错只挂在 lookup 的调用点。verify_citation /
# check_citations_in_text 等核验路径与 get_law / get_article 共用
# 的 find_law / snapshot_find_law 一律不带纠错——引注里写错法名
# 必须被标出来，不许被静默纠正放行（对照工具 thu 家在编造法名
# 上栽的正是这个跟头：模糊解析把编造名映射到真法还报 verified）。
# ---------------------------------------------------------------------------

# 拼音表（无声调、常用读音）：开发期由 pypinyin 对快照库标题＋条文
# 字集（2,868 字）离线生成后静态嵌入，运行时零依赖。仅用于编辑
# 距离并列时的第二级判别；表中没有的字按字面自身计，永不误配。
_PINYIN_SRC = (
    "一:yi|丁:ding|七:qi|万:wan|丈:zhang|三:san|上:shang|下:xia|不:bu|与:yu|丑:chou|专:zhuan|且:qie|世:shi|丘:qiu|丙:bing|业:ye|东:do"
    "ng|丝:si|丢:diu|两:liang|严:yan|丧:sang|个:ge|中:zhong|丰:feng|串:chuan|临:lin|丸:wan|丹:dan|为:wei|主:zhu|丽:li|举:ju|久:jiu|义"
    ":yi|之:zhi|乌:wu|乎:hu|乏:fa|乐:le|乔:qiao|乘:cheng|乙:yi|九:jiu|乞:qi|也:ye|习:xi|乡:xiang|书:shu|买:mai|乱:luan|乳:ru|了:le|予:"
    "yu|争:zheng|事:shi|二:er|于:yu|亏:kui|云:yun|互:hu|五:wu|井:jing|亚:ya|些:xie|亡:wang|交:jiao|亦:yi|产:chan|亩:mu|享:xiang|京:ji"
    "ng|亭:ting|亮:liang|亲:qin|亵:xie|人:ren|亿:yi|仁:ren|仅:jin|仆:pu|仇:chou|今:jin|介:jie|仍:reng|从:cong|仓:cang|仔:zai|他:ta|仗"
    ":zhang|付:fu|代:dai|令:ling|以:yi|仪:yi|们:men|仰:yang|仲:zhong|件:jian|价:jia|任:ren|份:fen|仿:fang|企:qi|伊:yi|伍:wu|伏:fu|伐:"
    "fa|休:xiu|众:zhong|优:you|伙:huo|会:hui|伞:san|伟:wei|传:chuan|伤:shang|伦:lun|伪:wei|伯:bo|估:gu|伴:ban|伸:shen|似:shi|但:dan|"
    "位:wei|低:di|住:zhu|佑:you|体:ti|何:he|余:yu|佛:fu|作:zuo|你:ni|佣:yong|佩:pei|佳:jia|使:shi|侈:chi|例:li|侍:shi|供:gong|依:yi|侦:"
    "zhen|侧:ce|侨:qiao|侮:wu|侵:qin|便:bian|促:cu|俗:su|俘:fu|保:bao|俟:qi|信:xin|俭:jian|修:xiu|俱:ju|倍:bei|倒:dao|候:hou|倚:yi|借:"
    "jie|倡:chang|债:zhai|值:zhi|倾:qing|假:jia|偏:pian|做:zuo|停:ting|健:jian|偶:ou|偷:tou|偿:chang|傍:bang|储:chu|催:cui|像:xiang"
    "|僚:liao|僻:pi|儋:dan|儿:er|允:yun|元:yuan|兄:xiong|充:chong|兆:zhao|先:xian|光:guang|克:ke|免:mian|兑:dui|兔:tu|党:dang|兜:dou"
    "|入:ru|全:quan|八:ba|公:gong|六:liu|兰:lan|共:gong|关:guan|兴:xing|兵:bing|其:qi|具:ju|典:dian|养:yang|兼:jian|兽:shou|冀:ji|内:"
    "nei|册:ce|再:zai|冒:mao|写:xie|军:jun|农:nong|冠:guan|冤:yuan|冬:dong|冰:bing|冲:chong|决:jue|况:kuang|冶:ye|冷:leng|冻:dong|净"
    ":jing|准:zhun|凌:ling|减:jian|凑:cou|凝:ning|几:ji|凡:fan|凤:feng|凭:ping|凰:huang|凶:xiong|凸:tu|凹:ao|出:chu|击:ji|函:han|凿:"
    "zao|刀:dao|刁:diao|刃:ren|分:fen|切:qie|刊:kan|刍:chu|刑:xing|划:hua|列:lie|刘:liu|则:ze|刚:gang|创:chuang|初:chu|删:shan|判:pa"
    "n|利:li|别:bie|刮:gua|到:dao|制:zhi|刷:shua|券:quan|刹:sha|刺:ci|刻:ke|剂:ji|削:xue|前:qian|剑:jian|剔:ti|剖:pou|剥:bo|剧:ju|剩:s"
    "heng|剪:jian|副:fu|割:ge|剽:piao|力:li|劝:quan|办:ban|功:gong|加:jia|务:wu|劣:lie|动:dong|助:zhu|努:nu|劫:jie|励:li|劳:lao|劾:he"
    "|势:shi|勇:yong|勉:mian|勋:xun|勒:lei|勘:kan|募:mu|勤:qin|勺:shao|勾:gou|匀:yun|包:bao|匕:bi|化:hua|北:bei|匝:za|匠:jiang|匪:fei"
    "|匹:pi|区:qu|医:yi|匾:bian|匿:ni|十:shi|千:qian|卅:sa|升:sheng|午:wu|卉:hui|半:ban|华:hua|协:xie|卓:zhuo|单:dan|卖:mai|南:nan|博:"
    "bo|占:zhan|卡:ka|卤:lu|卧:wo|卫:wei|印:yin|危:wei|即:ji|却:que|卵:luan|卷:juan|卸:xie|厂:chang|厅:ting|历:li|厉:li|压:ya|厌:yan|"
    "厕:ce|厘:li|厚:hou|原:yuan|厢:xiang|厦:sha|厨:chu|去:qu|县:xian|参:can|又:you|叉:cha|及:ji|友:you|双:shuang|反:fan|发:fa|取:qu|受"
    ":shou|变:bian|叙:xu|叛:pan|叠:die|口:kou|古:gu|句:ju|另:ling|只:zhi|叫:jiao|召:zhao|叭:ba|可:ke|台:tai|史:shi|右:you|叶:ye|号:ha"
    "o|司:si|吃:chi|各:ge|合:he|吉:ji|吊:diao|同:tong|名:ming|后:hou|吐:tu|向:xiang|吓:xia|吕:lv|吗:ma|吞:tun|否:fou|吧:ba|吨:dun|含:h"
    "an|听:ting|启:qi|吸:xi|吹:chui|吻:wen|吼:hou|吾:wu|呆:dai|呈:cheng|告:gao|员:yuan|周:zhou|味:wei|呼:hu|命:ming|咀:ju|和:he|咎:ji"
    "u|咖:ka|咨:zi|咬:yao|咳:ke|咸:xian|哀:ai|品:pin|哄:hong|哈:ha|哌:pai|响:xiang|哑:ya|哗:hua|哨:shao|哲:zhe|哺:bu|唆:suo|唇:chun|唛"
    ":ma|唤:huan|售:shou|唯:wei|唱:chang|商:shang|啡:fei|啤:pi|啮:nie|啶:ding|啸:xiao|喂:wei|善:shan|喇:la|喉:hou|喊:han|喜:xi|喧:xu"
    "an|喷:pen|嗜:shi|嘉:jia|嘱:zhu|嘴:zui|器:qi|噪:zao|四:si|回:hui|因:yin|团:tuan|囤:dun|园:yuan|困:kun|囱:cong|围:wei|固:gu|国:guo"
    "|图:tu|圃:pu|圆:yuan|圈:quan|土:tu|圣:sheng|在:zai|圩:wei|地:di|圳:zhen|场:chang|圾:ji|址:zhi|均:jun|坊:fang|坍:tan|坎:kan|坏:hu"
    "ai|坐:zuo|坑:keng|块:kuai|坚:jian|坛:tan|坝:ba|坞:wu|坟:fen|坠:zhui|坡:po|坦:tan|坪:ping|坯:pi|垂:chui|垃:la|垄:long|型:xing|垒:"
    "lei|垛:duo|垦:ken|垩:e|垫:dian|垮:kua|垸:yuan|埃:ai|埋:mai|城:cheng|域:yu|埠:bu|培:pei|基:ji|堂:tang|堆:dui|堑:qian|堡:bao|堤:di"
    "|堪:kan|堰:yan|堵:du|塌:ta|塑:su|塔:ta|塘:tang|塞:sai|填:tian|境:jing|墒:shang|墓:mu|墙:qiang|增:zeng|墟:xu|墨:mo|墩:dun|壁:bi|壅"
    ":yong|壤:rang|士:shi|壮:zhuang|声:sheng|壳:ke|处:chu|备:bei|复:fu|夏:xia|夕:xi|外:wai|多:duo|夜:ye|够:gou|大:da|天:tian|太:tai|"
    "夫:fu|央:yang|夯:hang|失:shi|头:tou|夸:kua|夹:jia|夺:duo|奇:qi|奉:feng|奋:fen|奏:zou|契:qi|奖:jiang|套:tao|奠:dian|奢:she|奥:ao|"
    "女:nv|奶:nai|奸:jian|好:hao|如:ru|妆:zhuang|妇:fu|妊:ren|妥:tuo|妨:fang|妹:mei|妻:qi|姊:zi|始:shi|姐:jie|姑:gu|姓:xing|委:wei|姻:"
    "yin|姿:zi|威:wei|娄:lou|娠:shen|娩:mian|娱:yu|娶:qu|娼:chang|婆:po|婚:hun|婴:ying|婿:xu|媒:mei|媳:xi|嫁:jia|嫌:xian|嫖:piao|嬉:x"
    "i|子:zi|孔:kong|孕:yun|字:zi|存:cun|孙:sun|孜:zi|孝:xiao|孟:meng|孢:bao|季:ji|孤:gu|学:xue|孳:zi|孵:fu|宁:ning|它:ta|宅:zhai|宇:y"
    "u|守:shou|安:an|完:wan|宏:hong|宗:zong|官:guan|定:ding|宜:yi|宝:bao|实:shi|宠:chong|审:shen|客:ke|宣:xuan|室:shi|宪:xian|宫:gon"
    "g|宰:zai|害:hai|宴:yan|宵:xiao|家:jia|容:rong|宽:kuan|宾:bin|宿:su|寄:ji|密:mi|富:fu|寒:han|寓:yu|察:cha|寡:gua|寨:zhai|寸:cun|对"
    ":dui|寺:si|寻:xun|导:dao|寿:shou|封:feng|射:she|将:jiang|尉:wei|尊:zun|小:xiao|少:shao|尔:er|尖:jian|尘:chen|尚:shang|尝:chang"
    "|就:jiu|尸:shi|尺:chi|尼:ni|尽:jin|尾:wei|尿:niao|局:ju|层:ceng|居:ju|屈:qu|届:jie|屋:wu|屏:ping|屑:xie|展:zhan|属:shu|屠:tu|屡:l"
    "v|履:lv|山:shan|屿:yu|岁:sui|岔:cha|岗:gang|岛:dao|岩:yan|岭:ling|岳:yue|岸:an|峡:xia|峰:feng|峻:jun|崇:chong|崖:ya|崩:beng|嵌:q"
    "ian|川:chuan|州:zhou|巡:xun|巢:chao|工:gong|左:zuo|巧:qiao|巨:ju|巩:gong|差:cha|己:ji|已:yi|巴:ba|巷:xiang|币:bi|市:shi|布:bu|帆"
    ":fan|师:shi|希:xi|帐:zhang|帕:pa|帜:zhi|帝:di|带:dai|帧:zhen|席:xi|帮:bang|常:chang|帽:mao|幅:fu|幕:mu|幢:chuang|干:gan|平:ping"
    "|年:nian|并:bing|幸:xing|幻:huan|幼:you|广:guang|庄:zhuang|庆:qing|庇:bi|床:chuang|序:xu|库:ku|应:ying|底:di|店:dian|庙:miao|府"
    ":fu|废:fei|度:du|座:zuo|庭:ting|康:kang|庸:yong|廉:lian|廊:lang|廓:kuo|延:yan|建:jian|开:kai|异:yi|弃:qi|弄:nong|弊:bi|式:shi|引"
    ":yin|弘:hong|弟:di|张:zhang|弥:mi|弧:hu|弩:nu|弯:wan|弱:ruo|弹:dan|强:qiang|归:gui|当:dang|录:lu|形:xing|彩:cai|彪:biao|彰:zhan"
    "g|影:ying|役:yi|彻:che|往:wang|征:zheng|径:jing|待:dai|徇:xun|很:hen|律:lv|徐:xu|徒:tu|徕:lai|得:de|徙:xi|御:yu|循:xun|微:wei|德:"
    "de|徽:hui|心:xin|必:bi|忆:yi|忌:ji|忍:ren|忏:chan|志:zhi|忘:wang|忙:mang|忠:zhong|忧:you|快:kuai|念:nian|忽:hu|怀:huai|态:tai|怂"
    ":song|怕:pa|怖:bu|思:si|怠:dai|急:ji|性:xing|总:zong|恋:lian|恐:kong|恒:heng|恕:shu|恢:hui|恤:xu|恨:hen|恩:en|恪:ke|息:xi|恰:qia"
    "|恳:ken|恶:e|恿:yong|悉:xi|悔:hui|悟:wu|悠:you|患:huan|悦:yue|悬:xuan|悼:dao|情:qing|惑:huo|惕:ti|惜:xi|惠:hui|惨:can|惩:cheng|惯"
    ":guan|想:xiang|愈:yu|愉:yu|意:yi|感:gan|愤:fen|愿:yuan|慈:ci|慌:huang|慎:shen|慢:man|慧:hui|慰:wei|憎:zeng|憨:han|懂:dong|懈:xi"
    "e|戊:wu|戍:shu|戏:xi|成:cheng|我:wo|戒:jie|或:huo|戗:qiang|战:zhan|戚:qi|截:jie|戳:chuo|戴:dai|户:hu|房:fang|所:suo|扇:shan|手:s"
    "hou|才:cai|扎:zha|扑:pu|扒:ba|打:da|扔:reng|托:tuo|扣:kou|执:zhi|扩:kuo|扫:sao|扬:yang|扭:niu|扮:ban|扯:che|扰:rao|扶:fu|批:pi|扼"
    ":e|找:zhao|承:cheng|技:ji|抄:chao|把:ba|抑:yi|抓:zhua|投:tou|抗:kang|折:zhe|抚:fu|抛:pao|抢:qiang|护:hu|报:bao|披:pi|抬:tai|抵:d"
    "i|抹:mo|押:ya|抽:chou|担:dan|拆:chai|拉:la|拌:ban|拍:pai|拐:guai|拒:ju|拓:tuo|拔:ba|拖:tuo|拘:ju|招:zhao|拟:ni|拢:long|拣:jian|拥"
    ":yong|拦:lan|拨:bo|择:ze|括:kuo|拯:zheng|拱:gong|拴:shuan|拷:kao|拼:pin|拽:zhuai|拾:shi|拿:na|持:chi|挂:gua|指:zhi|按:an|挑:tia"
    "o|挖:wa|挟:xie|挠:nao|挡:dang|挤:ji|挥:hui|挪:nuo|振:zhen|挺:ting|挽:wan|捆:kun|捉:zhuo|捍:han|捎:shao|捏:nie|捐:juan|捕:bu|捞:l"
    "ao|损:sun|捡:jian|换:huan|据:ju|捷:jie|捺:na|授:shou|掉:diao|掌:zhang|排:pai|掘:jue|掠:lve|探:tan|接:jie|控:kong|推:tui|掩:yan|"
    "措:cuo|掷:zhi|掺:can|描:miao|提:ti|插:cha|握:wo|揭:jie|援:yuan|揽:lan|搁:ge|搏:bo|搜:sou|搞:gao|搪:tang|搬:ban|搭:da|携:xie|摄:sh"
    "e|摆:bai|摇:yao|摊:tan|摘:zhai|摧:cui|摩:mo|摸:mo|摹:mo|撂:liao|撑:cheng|撒:sa|撕:si|撞:zhuang|撤:che|撬:qiao|播:bo|撮:cuo|撰:zh"
    "uan|擅:shan|操:cao|擎:qing|擦:ca|攀:pan|支:zhi|收:shou|改:gai|攻:gong|放:fang|政:zheng|故:gu|效:xiao|敌:di|敏:min|救:jiu|教:jia"
    "o|敢:gan|散:san|敦:dun|敬:jing|数:shu|敲:qiao|整:zheng|敷:fu|文:wen|斑:ban|斗:dou|料:liao|斜:xie|斤:jin|斥:chi|斧:fu|断:duan|斯:"
    "si|新:xin|方:fang|施:shi|旁:pang|旄:mao|旅:lv|旋:xuan|族:zu|旗:qi|无:wu|既:ji|日:ri|旦:dan|旧:jiu|旨:zhi|早:zao|旬:xun|旱:han|时:"
    "shi|旷:kuang|旺:wang|昆:kun|昌:chang|明:ming|昏:hun|易:yi|星:xing|映:ying|春:chun|是:shi|昼:zhou|显:xian|晋:jin|晒:shai|晓:xia"
    "o|晚:wan|晤:wu|晨:chen|普:pu|景:jing|晰:xi|晶:jing|智:zhi|晾:liang|暂:zan|暑:shu|暖:nuan|暗:an|暨:ji|暴:bao|曜:yao|曝:pu|曲:qu|曳"
    ":ye|更:geng|曾:ceng|替:ti|最:zui|月:yue|有:you|朋:peng|服:fu|朔:shuo|朗:lang|望:wang|朝:chao|期:qi|木:mu|未:wei|末:mo|本:ben|札:"
    "zha|术:shu|朴:pu|机:ji|朽:xiu|杀:sha|杂:za|权:quan|杆:gan|李:li|材:cai|村:cun|杖:zhang|杜:du|杞:qi|束:shu|杠:gang|条:tiao|来:lai"
    "|杨:yang|杭:hang|杰:jie|松:song|板:ban|极:ji|构:gou|枉:wang|析:xi|林:lin|枚:mei|果:guo|枝:zhi|枢:shu|枪:qiang|枯:ku|架:jia|某:mo"
    "u|染:ran|柔:rou|柜:gui|查:cha|柩:jiu|柬:jian|柯:ke|柱:zhu|柳:liu|柴:chai|栅:zha|标:biao|栈:zhan|栏:lan|树:shu|栓:shuan|栖:qi|校:"
    "xiao|株:zhu|样:yang|核:he|根:gen|格:ge|栽:zai|桂:gui|桅:wei|框:kuang|案:an|桌:zhuo|档:dang|桥:qiao|桨:jiang|桩:zhuang|桶:tong|"
    "梁:liang|梅:mei|梆:bang|梗:geng|梦:meng|梭:suo|梯:ti|械:xie|梳:shu|检:jian|棉:mian|棋:qi|棍:gun|棒:bang|棘:ji|棚:peng|森:sen|棱:"
    "leng|棺:guan|椅:yi|植:zhi|椎:chui|椒:jiao|楚:chu|楷:kai|楼:lou|概:gai|榄:lan|榜:bang|榴:liu|榻:ta|槌:chui|槛:kan|槽:cao|樟:zhan"
    "g|模:mo|横:heng|橄:gan|橙:cheng|橡:xiang|橱:chu|欠:qian|次:ci|欢:huan|欣:xin|欺:qi|款:kuan|歇:xie|歉:qian|歌:ge|止:zhi|正:zheng"
    "|此:ci|步:bu|武:wu|歧:qi|歪:wai|死:si|歼:jian|殊:shu|残:can|殓:lian|殖:zhi|殡:bin|殴:ou|段:duan|殷:yin|毁:hui|毋:wu|母:mu|每:mei|"
    "毒:du|比:bi|毕:bi|毖:bi|毗:pi|毛:mao|毫:hao|毯:tan|氏:shi|民:min|氓:mang|气:qi|氚:chuan|氛:fen|氟:fu|氡:dong|氢:qing|氦:hai|氧:ya"
    "ng|氨:an|氮:dan|氯:lv|氰:qing|水:shui|永:yong|汀:ting|汁:zhi|求:qiu|汇:hui|汉:han|汊:cha|汐:xi|汕:shan|汛:xun|汞:gong|江:jiang|"
    "池:chi|污:wu|汰:tai|汶:wen|汽:qi|沃:wo|沈:shen|沉:chen|沙:sha|沟:gou|没:mei|沤:ou|沥:li|沫:mo|河:he|沸:fei|油:you|治:zhi|沼:zhao|"
    "沽:gu|沾:zhan|沿:yan|泄:xie|泉:quan|泊:po|泔:gan|法:fa|泗:si|泛:fan|泞:ning|泡:pao|波:bo|泥:ni|注:zhu|泪:lei|泳:yong|泵:beng|泻:x"
    "ie|泼:po|泽:ze|洁:jie|洄:hui|洋:yang|洒:sa|洗:xi|洛:luo|洞:dong|津:jin|洪:hong|洲:zhou|活:huo|洼:wa|洽:qia|派:pai|流:liu|浅:qian"
    "|浆:jiang|测:ce|济:ji|浏:liu|浑:hun|浓:nong|浙:zhe|浚:jun|浦:pu|浩:hao|浪:lang|浬:li|浮:fu|浴:yu|海:hai|浸:jin|涂:tu|消:xiao|涉:s"
    "he|涌:yong|涝:lao|涡:wo|涤:di|润:run|涨:zhang|涯:ya|液:ye|涵:han|涸:he|淀:dian|淆:xiao|淋:lin|淘:tao|淡:dan|淤:yu|淫:yin|淮:huai"
    "|深:shen|淳:chun|混:hun|淹:yan|添:tian|清:qing|渍:zi|渎:du|渐:jian|渔:yu|渗:shen|渝:yu|渠:qu|渡:du|渣:zha|渤:bo|温:wen|港:gang|渲"
    ":xuan|游:you|湖:hu|湘:xiang|湛:zhan|湾:wan|湿:shi|溃:kui|溉:gai|源:yuan|溜:liu|溢:yi|溪:xi|溯:su|溴:xiu|溶:rong|溺:ni|滆:ge|滋:z"
    "i|滑:hua|滚:gun|滞:zhi|满:man|滤:lv|滥:lan|滦:luan|滨:bin|滩:tan|漂:piao|漆:qi|漏:lou|演:yan|漕:cao|漠:mo|漫:man|漱:shu|漳:zhang"
    "|潜:qian|潢:huang|潮:chao|潼:tong|澄:cheng|澎:peng|澡:zao|澳:ao|激:ji|濒:bin|灌:guan|火:huo|灭:mie|灯:deng|灰:hui|灵:ling|灶:za"
    "o|灾:zai|灿:can|炉:lu|炊:chui|炎:yan|炬:ju|炭:tan|炮:pao|炳:bing|炸:zha|点:dian|炼:lian|烁:shuo|烂:lan|烃:ting|烈:lie|烘:hong|烟"
    ":yan|烤:kao|烧:shao|烫:tang|热:re|烯:xi|烷:wan|烹:peng|烽:feng|焊:han|焚:fen|焦:jiao|焰:yan|然:ran|煅:duan|煤:mei|照:zhao|煽:sh"
    "an|熄:xi|熊:xiong|熏:xun|熔:rong|熟:shu|熬:ao|燃:ran|燕:yan|燥:zao|爆:bao|爪:zhao|爬:pa|爱:ai|父:fu|片:pian|版:ban|牌:pai|牙:ya|"
    "牛:niu|牟:mou|牡:mu|牢:lao|牧:mu|物:wu|牲:sheng|牵:qian|特:te|牺:xi|犊:du|犬:quan|犯:fan|状:zhuang|狂:kuang|狗:gou|狩:shou|独:du"
    "|狭:xia|狱:yu|猎:lie|猛:meng|猝:cu|猥:wei|猩:xing|猪:zhu|猫:mao|献:xian|猴:hou|猿:yuan|獭:ta|玄:xuan|率:lv|玉:yu|王:wang|玛:ma|玩"
    ":wan|环:huan|现:xian|玳:dai|玷:dian|玺:xi|玻:bo|珂:ke|珍:zhen|珠:zhu|班:ban|球:qiu|理:li|琴:qin|琼:qiong|瑁:mao|瑕:xia|瑙:nao|瑶"
    ":yao|璃:li|瓜:gua|瓦:wa|瓶:ping|瓷:ci|甄:zhen|甘:gan|甚:shen|生:sheng|用:yong|甩:shuai|田:tian|由:you|甲:jia|申:shen|电:dian|男"
    ":nan|画:hua|畅:chang|界:jie|畏:wei|留:liu|畜:chu|略:lve|番:fan|畴:chou|畸:ji|疆:jiang|疏:shu|疑:yi|疗:liao|疟:nve|疫:yi|疯:feng"
    "|疲:pi|疵:ci|疹:zhen|疼:teng|疽:ju|疾:ji|病:bing|症:zheng|痕:hen|痘:dou|痛:tong|痢:li|痫:xian|痰:tan|瘟:wen|瘠:ji|瘤:liu|瘾:yin|"
    "癌:ai|癖:pi|癫:dian|登:deng|白:bai|百:bai|皂:zao|的:de|皆:jie|皮:pi|皿:min|盆:pen|盈:ying|益:yi|盏:zhan|盐:yan|监:jian|盒:he|盔:k"
    "ui|盖:gai|盗:dao|盘:pan|盛:sheng|盟:meng|盥:guan|目:mu|盲:mang|直:zhi|相:xiang|盼:pan|盾:dun|省:sheng|看:kan|真:zhen|眠:mian|眷"
    ":juan|眼:yan|着:zhe|睡:shui|督:du|睦:mu|睹:du|瞒:man|瞬:shun|瞭:liao|瞻:zhan|矗:chu|矛:mao|知:zhi|矫:jiao|短:duan|石:shi|矸:gan"
    "|矽:xi|矾:fan|矿:kuang|码:ma|砂:sha|砍:kan|研:yan|砖:zhuan|砝:fa|砧:zhen|破:po|砷:shen|砸:za|砾:li|础:chu|硅:gui|硐:dong|硒:xi|硕"
    ":shuo|硝:xiao|硫:liu|硬:ying|确:que|硼:peng|碍:ai|碎:sui|碑:bei|碘:dian|碧:bi|碰:peng|碱:jian|碲:di|碳:tan|碾:nian|磁:ci|磅:ban"
    "g|磋:cuo|磨:mo|磷:lin|礁:jiao|礴:bo|示:shi|礼:li|社:she|祉:zhi|祖:zu|祛:qu|祝:zhu|神:shen|祠:ci|祥:xiang|票:piao|祭:ji|祸:huo|禀:"
    "bing|禁:jin|福:fu|离:li|禽:qin|秀:xiu|私:si|秆:gan|秉:bing|秋:qiu|种:zhong|科:ke|秒:miao|秘:mi|租:zu|秤:cheng|秧:yang|秩:zhi|积:"
    "ji|称:cheng|秸:jie|移:yi|秽:hui|稀:xi|程:cheng|税:shui|稞:ke|稠:chou|稳:wen|稻:dao|稽:ji|稿:gao|穆:mu|穗:sui|穴:xue|究:jiu|穷:qi"
    "ong|空:kong|穿:chuan|突:tu|窃:qie|窄:zhai|窑:yao|窖:jiao|窗:chuang|窜:cuan|窝:wo|窟:ku|窥:kui|窨:xun|立:li|竖:shu|站:zhan|竞:ji"
    "ng|章:zhang|竣:jun|童:tong|竭:jie|端:duan|竹:zhu|笋:sun|笔:bi|笛:di|符:fu|笨:ben|第:di|笺:jian|笼:long|等:deng|筋:jin|筏:fa|筐:k"
    "uang|筑:zhu|筒:tong|答:da|策:ce|筛:shai|筝:zheng|筷:kuai|筹:chou|签:qian|简:jian|箔:bo|算:suan|管:guan|箭:jian|箱:xiang|篆:zhu"
    "an|篇:pian|篡:cuan|篮:lan|篱:li|篷:peng|簧:huang|簿:bu|籍:ji|米:mi|类:lei|籽:zi|粉:fen|粒:li|粗:cu|粘:zhan|粟:su|粤:yue|粪:fen|粮"
    ":liang|粱:liang|精:jing|糖:tang|糙:cao|系:xi|紊:wen|素:su|索:suo|紧:jin|紫:zi|累:lei|繁:fan|纂:zuan|纠:jiu|红:hong|纤:xian|约:y"
    "ue|级:ji|纪:ji|纬:wei|纯:chun|纲:gang|纳:na|纵:zong|纷:fen|纸:zhi|纹:wen|纺:fang|纽:niu|纾:shu|线:xian|练:lian|组:zu|细:xi|织:zh"
    "i|终:zhong|绍:shao|经:jing|绑:bang|绒:rong|结:jie|绕:rao|绘:hui|给:gei|络:luo|绝:jue|绞:jiao|统:tong|绥:sui|继:ji|绩:ji|绪:xu|续"
    ":xu|绳:sheng|维:wei|绵:mian|绶:shou|综:zong|绾:wan|绿:lv|缀:zhui|缄:jian|缅:mian|缆:lan|缉:ji|缓:huan|缔:di|编:bian|缘:yuan|缝:"
    "feng|缠:chan|缩:suo|缮:shan|缴:jiao|缸:gang|缺:que|罂:ying|罐:guan|网:wang|罕:han|罗:luo|罚:fa|罢:ba|罩:zhao|罪:zui|置:zhi|署:s"
    "hu|罹:li|羁:ji|羊:yang|美:mei|群:qun|羽:yu|翁:weng|翅:chi|翔:xiang|翘:qiao|翠:cui|翡:fei|翻:fan|翼:yi|耀:yao|老:lao|考:kao|者:zh"
    "e|而:er|耐:nai|耕:geng|耗:hao|耶:ye|耻:chi|耽:dan|聋:long|职:zhi|联:lian|聘:pin|聚:ju|肃:su|肄:yi|肇:zhao|肉:rou|肖:xiao|肝:gan|"
    "肠:chang|股:gu|肢:zhi|肤:fu|肥:fei|肩:jian|肯:ken|育:yu|肺:fei|肽:tai|肾:shen|肿:zhong|胀:zhang|胁:xie|背:bei|胎:tai|胖:pang|胚:"
    "pei|胜:sheng|胞:bao|胡:hu|胰:yi|胴:dong|胶:jiao|胸:xiong|胺:an|能:neng|脂:zhi|脆:cui|脉:mai|脊:ji|脏:zang|脐:qi|脑:nao|脓:nong|"
    "脚:jiao|脱:tuo|脸:lian|腈:jing|腐:fu|腕:wan|腮:sai|腰:yao|腹:fu|腺:xian|腾:teng|膀:bang|膏:gao|膜:mo|膨:peng|膳:shan|臂:bi|臣:ch"
    "en|自:zi|臭:chou|至:zhi|致:zhi|舆:yu|舌:she|舍:she|舒:shu|舞:wu|舟:zhou|航:hang|般:ban|舰:jian|舱:cang|舵:duo|舶:bo|舷:xian|船:c"
    "huan|艇:ting|艏:shou|艘:sou|良:liang|艰:jian|色:se|艺:yi|艾:ai|节:jie|芒:mang|芘:pi|芜:wu|芡:qian|芦:lu|芬:fen|芯:xin|花:hua|芽:"
    "ya|苇:wei|苏:su|苕:shao|苗:miao|若:ruo|苦:ku|苯:ben|英:ying|苷:gan|茂:mao|范:fan|茄:jia|茅:mao|茎:jing|茧:jian|茬:cha|茶:cha|荆:"
    "jing|草:cao|荐:jian|荒:huang|荞:qiao|荡:dang|荣:rong|药:yao|荷:he|荻:di|莞:guan|莲:lian|获:huo|菌:jun|菜:cai|菱:ling|萃:cui|萄:"
    "tao|萤:ying|营:ying|落:luo|著:zhu|葛:ge|葡:pu|董:dong|葫:hu|葬:zang|蒙:meng|蒸:zheng|蓄:xu|蓝:lan|蔑:mie|蔓:man|蔬:shu|蔽:bi|蕊:"
    "rui|蕴:yun|薄:bao|薪:xin|薯:shu|藉:ji|藏:cang|藕:ou|藤:teng|藻:zao|虏:lu|虐:nve|虑:lv|虚:xu|虞:yu|虫:chong|虱:shi|虹:hong|虽:sui"
    "|虾:xia|蚀:shi|蚊:wen|蚌:bang|蚕:can|蚤:zao|蚴:you|蚶:han|蛀:zhu|蛇:she|蛋:dan|蛎:li|蛏:cheng|蛤:ha|蛭:zhi|蜂:feng|蜚:fei|蜡:la|"
    "蜱:pi|蝇:ying|螂:lang|融:rong|螨:man|螯:ao|螺:luo|蟑:zhang|蟹:xie|蠊:lian|血:xue|衅:xin|行:xing|衍:yan|衔:xian|街:jie|衡:heng|衣"
    ":yi|补:bu|表:biao|衬:chen|衰:shuai|袋:dai|袒:tan|袖:xiu|被:bei|袭:xi|裁:cai|裂:lie|装:zhuang|裔:yi|裕:yu|裙:qun|裸:luo|裹:guo|褒"
    ":bao|褥:ru|褪:tui|西:xi|要:yao|覆:fu|见:jian|观:guan|规:gui|视:shi|觇:chan|览:lan|觉:jue|觐:jin|角:jiao|解:jie|触:chu|言:yan|誉:"
    "yu|誓:shi|警:jing|计:ji|订:ding|认:ren|讨:tao|让:rang|讫:qi|训:xun|议:yi|讯:xun|记:ji|讲:jiang|许:xu|讹:e|论:lun|讼:song|设:she|"
    "访:fang|证:zheng|评:ping|识:shi|诈:zha|诉:su|诊:zhen|诋:di|词:ci|译:yi|试:shi|诗:shi|诚:cheng|话:hua|诞:dan|询:xun|诣:yi|该:gai|"
    "详:xiang|诫:jie|诬:wu|语:yu|误:wu|诱:you|诲:hui|说:shuo|诵:song|请:qing|诸:zhu|诺:nuo|读:du|诽:fei|课:ke|诿:wei|谁:shui|调:diao|"
    "谅:liang|谈:tan|谊:yi|谋:mou|谍:die|谎:huang|谐:xie|谓:wei|谢:xie|谣:yao|谤:bang|谦:qian|谨:jin|谩:man|谱:pu|谴:qian|谷:gu|豁:hu"
    "o|豆:dou|豚:tun|象:xiang|豪:hao|貌:mao|贝:bei|负:fu|贡:gong|财:cai|责:ze|贤:xian|败:bai|账:zhang|货:huo|质:zhi|贩:fan|贪:tan|贫:"
    "pin|贬:bian|购:gou|贮:zhu|贯:guan|贴:tie|贵:gui|贷:dai|贸:mao|费:fei|贺:he|贻:yi|贼:zei|贿:hui|赁:lin|赂:lu|赃:zang|资:zi|赈:zhe"
    "n|赊:she|赋:fu|赌:du|赎:shu|赏:shang|赔:pei|赖:lai|赛:sai|赞:zan|赠:zeng|赡:shan|赢:ying|赤:chi|赦:she|赫:he|走:zou|赴:fu|赶:gan"
    "|起:qi|超:chao|越:yue|趋:qu|趣:qu|足:zu|趸:dun|跃:yue|跌:die|跑:pao|距:ju|跟:gen|跨:kua|路:lu|跳:tiao|践:jian|踊:yong|踏:ta|踪:zo"
    "ng|蹄:ti|蹈:dao|蹲:dun|躁:zao|身:shen|躯:qu|躲:duo|车:che|轧:ya|轨:gui|转:zhuan|轮:lun|软:ruan|轰:hong|轴:zhou|轻:qing|载:zai|轿"
    ":jiao|较:jiao|辅:fu|辆:liang|辈:bei|辉:hui|辊:gun|辍:chuo|辐:fu|辑:ji|输:shu|辕:yuan|辖:xia|辙:zhe|辛:xin|辜:gu|辞:ci|辟:pi|辨:b"
    "ian|辩:bian|辱:ru|边:bian|辽:liao|达:da|迁:qian|迄:qi|迅:xun|过:guo|迈:mai|迎:ying|运:yun|近:jin|返:fan|还:hai|这:zhe|进:jin|远:"
    "yuan|违:wei|连:lian|迟:chi|迪:di|迫:po|述:shu|迷:mi|迹:ji|追:zhui|退:tui|送:song|适:shi|逃:tao|逆:ni|选:xuan|透:tou|逐:zhu|递:di"
    "|途:tu|逗:dou|通:tong|逝:shi|逞:cheng|速:su|造:zao|逢:feng|逮:dai|逸:yi|逻:luo|逼:bi|逾:yu|遁:dun|遂:sui|遇:yu|遍:bian|遏:e|道:da"
    "o|遗:yi|遛:liu|遣:qian|遥:yao|遭:zao|遮:zhe|遴:lin|遵:zun|避:bi|邀:yao|邓:deng|那:na|邪:xie|邮:you|邸:di|邻:lin|郁:yu|郊:jiao|郑:"
    "zheng|部:bu|都:dou|酌:zhuo|配:pei|酐:gan|酒:jiu|酗:xu|酚:fen|酝:yun|酬:chou|酮:tong|酯:zhi|酰:xian|酵:jiao|酶:mei|酷:ku|酸:suan"
    "|酿:niang|醇:chun|醉:zui|醋:cu|醒:xing|醚:mi|醛:quan|采:cai|释:shi|里:li|重:zhong|野:ye|量:liang|金:jin|鉴:jian|钆:ga|钇:yi|针:z"
    "hen|钉:ding|钌:liao|钍:tu|钐:shan|钒:fan|钓:diao|钕:nv|钚:bu|钛:tai|钞:chao|钟:zhong|钠:na|钢:gang|钥:yao|钨:wu|钩:gou|钪:kang|"
    "钬:huo|钯:ba|钱:qian|钳:qian|钴:gu|钷:po|钻:zuan|钼:mu|钽:tan|钾:jia|铀:you|铁:tie|铂:bo|铃:ling|铅:qian|铈:shi|铊:ta|铋:bi|铌:ni"
    "|铍:pi|铐:kao|铑:lao|铒:er|铕:you|铜:tong|铝:lv|铟:yin|铡:zha|铣:xi|铥:diu|铪:ha|铬:ge|铭:ming|铯:se|铰:jiao|铱:yi|铲:chan|铳:cho"
    "ng|铵:an|银:yin|铷:ru|铸:zhu|铺:pu|铼:lai|铽:te|链:lian|销:xiao|锁:suo|锂:li|锅:guo|锆:gao|锇:e|锈:xiu|锋:feng|锌:xin|锐:rui|锑:t"
    "i|锗:zhe|错:cuo|锚:mao|锡:xi|锣:luo|锤:chui|锭:ding|键:jian|锯:ju|锰:meng|锶:si|锻:duan|镀:du|镁:mei|镇:zhen|镉:ge|镍:nie|镓:jia"
    "|镜:jing|镝:di|镣:liao|镥:lu|镧:lan|镨:pu|镱:yi|镶:xiang|长:zhang|门:men|闪:shan|闭:bi|问:wen|闯:chuang|闲:xian|间:jian|闸:zha|"
    "闹:nao|闻:wen|闽:min|阀:fa|阂:he|阅:yue|阈:yu|阐:chan|阔:kuo|阜:fu|队:dui|防:fang|阳:yang|阴:yin|阵:zhen|阶:jie|阻:zu|阿:a|附:fu|"
    "际:ji|陆:lu|陈:chen|陋:lou|陌:mo|降:jiang|限:xian|陕:shan|陡:dou|院:yuan|除:chu|险:xian|陪:pei|陵:ling|陶:tao|陷:xian|隆:long|随"
    ":sui|隐:yin|隔:ge|隘:ai|隙:xi|障:zhang|隧:sui|隶:li|难:nan|雄:xiong|集:ji|雇:gu|雌:ci|雏:chu|雕:diao|雨:yu|雪:xue|零:ling|雷:lei"
    "|雹:bao|雾:wu|需:xu|震:zhen|霉:mei|霍:huo|霓:ni|霜:shuang|霞:xia|露:lu|霸:ba|霾:mai|青:qing|静:jing|非:fei|靠:kao|面:mian|革:ge|"
    "靴:xue|靶:ba|鞋:xie|鞍:an|鞭:bian|韧:ren|韭:jiu|音:yin|韶:shao|页:ye|顶:ding|顷:qing|项:xiang|顺:shun|须:xu|顽:wan|顾:gu|顿:dun|"
    "颁:ban|预:yu|领:ling|频:pin|颖:ying|颗:ke|题:ti|颜:yan|额:e|颠:dian|颤:chan|风:feng|飘:piao|飞:fei|食:shi|餐:can|饥:ji|饪:ren|饭:"
    "fan|饮:yin|饰:shi|饱:bao|饲:si|饵:er|饷:xiang|饿:e|馆:guan|馈:kui|馏:liu|首:shou|香:xiang|馨:xin|马:ma|驭:yu|驮:tuo|驯:xun|驰:ch"
    "i|驱:qu|驳:bo|驶:shi|驻:zhu|驾:jia|骂:ma|骆:luo|验:yan|骑:qi|骗:pian|骚:sao|骤:zhou|骨:gu|骸:hai|髓:sui|高:gao|魂:hun|魄:po|魔:mo"
    "|鱼:yu|鱿:you|鲁:lu|鲂:fang|鲆:ping|鲌:ba|鲍:bao|鲐:tai|鲑:gui|鲚:ji|鲛:jiao|鲜:xian|鲟:xun|鲡:li|鲢:lian|鲤:li|鲥:shi|鲫:ji|鲮:l"
    "ing|鲱:fei|鲳:chang|鲴:gu|鲵:ni|鲷:diao|鲸:jing|鲹:shen|鲽:die|鳊:bian|鳍:qi|鳎:ta|鳓:le|鳕:xue|鳖:bie|鳗:man|鳙:yong|鳜:gui|鳝:"
    "shan|鳞:lin|鸟:niao|鸡:ji|鸣:ming|鸦:ya|鸭:ya|鹅:e|鹰:ying|麒:qi|麟:lin|麦:mai|麻:ma|黄:huang|黏:nian|黑:hei|默:mo|鼓:gu|鼠:shu|"
    "鼻:bi|齐:qi|齿:chi|龄:ling|龈:ken|龋:qu|龙:long|龟:gui"
)


_PINYIN_CACHE = None


def _pinyin_of(ch):
    global _PINYIN_CACHE
    if _PINYIN_CACHE is None:
        table = {}
        for pair in _PINYIN_SRC.split("|"):
            if pair:
                key, _, py = pair.partition(":")
                if key and py:
                    table[key] = py
        _PINYIN_CACHE = table
    return _PINYIN_CACHE.get(ch, ch)


def _syllables(text):
    return [_pinyin_of(ch) for ch in text]


def _edit_distance(a, b):
    """Damerau-Levenshtein（OSA）距离；字符串（逐字）与音节序列通用。

    867：旧实现为纯 Levenshtein，相邻字序颠倒（品药/防消/筑建这类
    typo）要付 2 次替换的代价，在短法名上直接越过阈值或与无关法名
    并列成 tie（865 typo 11/20 的主因之一）。OSA 把一次相邻换位计
    为 1，与「打错字」的真实生成方式对齐。
    """
    la, lb = len(a), len(b)
    if la == 0:
        return lb
    if lb == 0:
        return la
    prev2 = None
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        cur = [i] + [0] * lb
        ai = a[i - 1]
        for j in range(1, lb + 1):
            cost = 0 if ai == b[j - 1] else 1
            best = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if (prev2 is not None and j > 1
                    and ai == b[j - 2] and a[i - 2] == b[j - 1]):
                best = min(best, prev2[j - 2] + 1)
            cur[j] = best
        prev2, prev = prev, cur
    return prev[lb]


_NAME_FORMS_CACHE = None


def _law_name_forms():
    """已知法名全集：{名称形式: {法规全名, ...}}，进程内缓存。

    来源：本地快照库标题、flk 枚举清单（enumeration.jsonl）、moj
    标题索引、常用简称表。每部法规登记全名与去「中华人民共和国」
    前缀的短名两种形式。任一来源缺失/损坏时跳过该来源（纠错能力
    降级，不影响精确查询）；gzk 国家规章库无本地名录，不在全集内。
    """
    global _NAME_FORMS_CACHE
    if _NAME_FORMS_CACHE is not None:
        return _NAME_FORMS_CACHE
    forms = {}

    def _add(title):
        title = (title or "").strip()
        if not title:
            return
        forms.setdefault(title, set()).add(title)
        short = title[len("中华人民共和国"):] \
            if title.startswith("中华人民共和国") else title
        if short and short != title:
            forms.setdefault(short, set()).add(title)

    conn = _open_snapshot()
    if conn is not None:
        try:
            for row in conn.execute("SELECT DISTINCT title FROM laws"):
                _add(row[0])
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    enum_path = os.path.join(os.path.dirname(snapshot_db_path()),
                             "enumeration.jsonl")
    try:
        with open(enum_path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    _add(json.loads(line).get("title"))
    except (OSError, ValueError):
        pass
    for item in _moj_index_items():
        _add(item.get("title"))
    for alias, target in _LAW_ALIASES.items():
        _add(target)
        if alias:
            forms.setdefault(alias, set()).add(target)
    _NAME_FORMS_CACHE = forms
    return forms


_NEAR_NAME_SHORT_LEN = 4        # 法名 ≤4 字算短名
_NEAR_NAME_DIST_SHORT = 1       # 短名至多差 1 字
_NEAR_NAME_DIST_LONG = 2        # 更长的法名至多差 2 字


def _near_name_match(name):
    """在已知法名全集里给 name 找近名 → (kind, payload)。

    kind == "corrected"：payload = (法规全名, 命中的名称形式)——
        编辑距离最小且唯一胜出（并列时拼音距离唯一胜出）；
    kind == "tie"：payload = [法规全名, ...]，分不出唯一胜者；
    kind == "none"：阈值内无候选。
    """
    forms = _law_name_forms()
    if not forms or not name:
        return "none", None
    # 867 修订：曾试过把纯简称形式（如「个保法」）排除出纠错候选，
    # 防 865 的「环保法/险保法被个保法吸走」；实测该排除误伤
    # 「民诉珐→民诉法」「销法→消法」等以简称形式为目标的正当纠错
    # （843b 回归 5 例失败），且 865 的两个误吸输入如今一个已进
    # 简称表（解析层先行命中、根本到不了纠错）、一个由下方字符
    # 多重集并列规则正确分流，故撤销排除、只保留多重集规则。
    limit = (_NEAR_NAME_DIST_SHORT if len(name) <= _NEAR_NAME_SHORT_LEN
             else _NEAR_NAME_DIST_LONG)
    best = {}   # 法规全名 -> (最小编辑距离, 命中形式)
    for form, titles in forms.items():
        if abs(len(form) - len(name)) > limit:
            continue
        dist = _edit_distance(name, form)
        if dist == 0 or dist > limit:
            continue
        for title in titles:
            if title not in best or dist < best[title][0]:
                best[title] = (dist, form)
    if not best:
        return "none", None
    dmin = min(d for d, _f in best.values())
    winners = {t: f for t, (d, f) in best.items() if d == dmin}
    if len(winners) == 1:
        title, form = next(iter(winners.items()))
        return "corrected", (title, form)
    # 867：并列时先看字符多重集——与输入恰为同一组字的候选是字序
    # 颠倒型 typo 的目标（如「品药」↔「药品」），优先于拼音判读。
    same_bag = [t for t, f in winners.items()
                if sorted(f) == sorted(name)]
    if len(same_bag) == 1:
        title = same_bag[0]
        return "corrected", (title, winners[title])
    # 编辑距离并列：按拼音距离分胜负（同音错字优先），仍并列才交人挑
    name_syl = _syllables(name)
    phon = {t: _edit_distance(name_syl, _syllables(f))
            for t, f in winners.items()}
    pmin = min(phon.values())
    phon_winners = [t for t, p in phon.items() if p == pmin]
    if len(phon_winners) == 1:
        title = phon_winners[0]
        return "corrected", (title, winners[title])
    return "tie", sorted(winners)


def _lookup_resolvable(name):
    """现有三路解析任一能定位即视为可解析（纠错不启用）。

    口径：快照腿的包含命中是短名解析的正道（如「民法典」⊂
    《中华人民共和国民法典》），照常算可解析；flk 实时腿只认
    exact——其实时包含命中会把「刑罚」这类错名吸到标题中段恰好
    含该词的司法解释批复上（842 开发集 lookup-153 的实际失败
    模式），那不是用户要找的法，不许它挡住纠错。flk 仅包含命中
    时先过纠错门：无近名候选或并列时仍按原名走原分发，包含命中
    的原有行为不变。
    任一通道报错（网络异常等）时保守地视为可解析：通道不健康时
    绝不触发纠错，宁可让后续正式查询按原口径报错。
    """
    try:
        hit = snapshot_find_law(name)
        # 867：快照只命中带实质后缀的衍生法规（title_extra）不算
        # 可解析——那是另一部法，须让 flk 精确腿与纠错门有机会。
        if hit is not None and not hit.get("title_extra"):
            return True
    except Exception:
        return True
    try:
        found, match = find_law(name)
        if found is not None and match == "exact":
            return True
        if found is not None and match == "contains" \
                and not found.get("name_mismatch"):
            # 867：短名全等也算可解析——去「中华人民共和国」前缀
            # 后与所给名一字不差（如《国家公园法》），只是 flk 命中
            # 方式记为 contains；旧实现不认，纠错门随后用近名 tie
            # 把它误杀（865 lookup-119 实测）。「刑罚」式中段沾边
            # 达不到短名全等，不受影响。
            short = found["title"][len("中华人民共和国"):] \
                if found["title"].startswith("中华人民共和国") \
                else found["title"]
            if short == name:
                return True
    except Exception:
        return True
    try:
        return moj_find_law(name) is not None
    except Exception:
        return True


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
    # 867 增补（865 alias 未解析 8 题中《网安法》一词占 4 题）：
    "网安法": "中华人民共和国网络安全法",
    "环保法": "中华人民共和国环境保护法",
    "数安法": "中华人民共和国数据安全法",
    "环评法": "中华人民共和国环境影响评价法",
    "大气法": "中华人民共和国大气污染防治法",
    "水污染法": "中华人民共和国水污染防治法",
}

_LOOKUP_ARTICLE_RE = re.compile(
    r"第?\s*([0-9]+|[零一二三四五六七八九十百千两]+)\s*条"
    r"(?:\s*之\s*([0-9]+|[零一二三四五六七八九十百千两]+))?")
_LOOKUP_FILLER_RE = re.compile(
    r"(帮我|请)?(查一?下|查询|看看|看一下|搜索|检索)?|是什么|怎么规定|"
    r"规定的?|原文|内容|全文|讲的是什么|说的是什么|了|吗|呢|[？?。，、\s]")
# 867：「的」不再全局删除——它会把法名内部的「的」一并删掉
# （「关于…若干问题的解释」→「…若干问题解释」，865 lookup 长名
# 司法解释因此查无此法）；句尾的「的」由 parse_lookup_query 末尾
# rstrip 处理。


def _expand_inner_law_name(iname):
    """内嵌法名展开：简称表直查，否则在已知法名全集里找唯一对应
    全名（如「渔业法」→「中华人民共和国渔业法」）。"""
    full = _LAW_ALIASES.get(iname)
    if full:
        return full
    titles = _law_name_forms().get(iname) or set()
    if len(titles) == 1:
        return next(iter(titles))
    return None


def parse_lookup_query(query):
    """解析一句话查询 → (法名, 条号num|None, sub|None, 解析方式)。

    支持：《民法典》第五百七十七条 / 民法典577条 / 劳动合同法第三十九条
    是什么 / 民诉法第64条。解析不了条号时只回法名（走法规级查询）。
    """
    q = _fw_norm((query or "").strip()).replace("條", "条")
    if not q:
        raise SourceError("query 不能为空")
    j = q.find("《")
    if j >= 0:
        # 867：书名号按嵌套深度取最外层——旧 [^》] 正则在《山西省
        # 实施《渔业法》办法》这类内嵌法名处截成内层「渔业法」。
        law_name, end = _scan_book_title(q, j)
        if law_name:
            law_name = law_name.strip()
            inner = law_name.find("《")
            if inner >= 0:
                iname, iend = _scan_book_title(law_name, inner)
                if iname:
                    full = _expand_inner_law_name(iname.strip())
                    if full:
                        law_name = (law_name[:inner] + "《" + full + "》"
                                    + law_name[iend:])
            else:
                # 867：裸内嵌形态——「山西省实施《渔业法》办法第22条」
                # 书名号只包内层法，前缀（…实施）与后缀（办法）是
                # 外层法名的组成部分，只取内层会查错法。前后缀形态
                # 明确且内层能展开为唯一全名时，重构外层全名。
                # 前缀先剥填充词（「查一下新疆…实施」→「新疆…实施」，
                # 865 lookup-136 实测填充词混进了重构法名）。
                prefix = _LOOKUP_FILLER_RE.sub("", q[:j]).strip(" 《》")
                m_suf = re.match(r"(办法|条例|规定|细则|规则)",
                                 q[end:])
                full = _expand_inner_law_name(law_name) \
                    if prefix.endswith(("实施", "执行", "贯彻")) \
                    and m_suf else None
                if not full and prefix.endswith(("实施", "执行", "贯彻")) \
                        and m_suf:
                    # 本地全集没有内层法（如渔业法不在快照内）时，
                    # 用 flk 官方检索取其全名再重构。
                    try:
                        found, _m = find_law(law_name)
                        if found is not None:
                            full = found["title"]
                    except Exception:
                        full = None
                if full and m_suf:
                    law_name = (prefix + "《" + full + "》"
                                + m_suf.group(1))
                    rest = q[end + len(m_suf.group(1)):]
                    am = _LOOKUP_ARTICLE_RE.search(rest)
                    if am:
                        return law_name, cn_to_int(am.group(1)), \
                            cn_to_int(am.group(2)) if am.group(2) \
                            else None, "bracket"
                    return law_name, None, None, "bracket"
            rest = q[:j] + q[end:]
            am = _LOOKUP_ARTICLE_RE.search(rest)
            if am:
                return law_name, cn_to_int(am.group(1)), \
                    cn_to_int(am.group(2)) if am.group(2) else None, "bracket"
            return law_name, None, None, "bracket"
    am = _LOOKUP_ARTICLE_RE.search(q)
    if am:
        law_part = q[:am.start()]
        law_name = _LOOKUP_FILLER_RE.sub("", law_part).strip("《》 ")
        law_name = law_name.rstrip("的")
        if law_name:
            return law_name, cn_to_int(am.group(1)), \
                cn_to_int(am.group(2)) if am.group(2) else None, "inline"
    law_name = _LOOKUP_FILLER_RE.sub("", q).strip("《》 ")
    law_name = law_name.rstrip("的")
    if not law_name:
        raise SourceError(f"无法从「{query}」中解析出法名")
    return law_name, None, None, "law_only"


_HUMAN_REVIEW_BASE = [
    "效力时效性：结论基于工具标注的通道与取数时间，个案适用前建议再核对一次官方源",
    "论点支持关系不在本工具核验范围：条文真实、现行，不等于它支持你的具体论点（可用 check_support 做启发式初筛，最终须人工判断）",
    "具体适用须结合案件事实、地方规定与司法解释，本结果不构成法律意见",
]


def _flk_fallback_correction(name):
    """本地全集无候选时，用 flk 官方检索为写错的法名找候选（867）。

    两路取候选：①单字删除变体精确查——治衍字/叠字型错别字
    （「海洋环境保护护法」删一「护」即本名）；②首部分词查——治
    缺字型（官方引擎对「湿 保护法」能召回《湿地保护法》，整串
    精确查与模糊查都召回不了）。候选一律过与本地纠错同一编辑
    距离门槛，唯一最小才采纳；并列或超阈弃权（交回 none 口径）。
    返回 (法规全名, 命中形式) 或 None。
    """
    limit = (_NEAR_NAME_DIST_SHORT if len(name) <= _NEAR_NAME_SHORT_LEN
             else _NEAR_NAME_DIST_LONG)
    cands = {}

    def _consider(title):
        title = (title or "").strip()
        if not title or title == name:
            return
        short = title[len("中华人民共和国"):] \
            if title.startswith("中华人民共和国") else title
        d = min(_edit_distance(name, title),
                _edit_distance(name, short))
        if 1 <= d <= limit and d < cands.get(title, 99):
            cands[title] = d

    for i in range(len(name)):
        variant = name[:i] + name[i + 1:]
        if len(variant) < 2:
            continue
        try:
            found, match = find_law(variant)
        except Exception:
            found, match = None, None
        if found is not None and match == "exact":
            _consider(found["title"])
    for k in (1, 2):
        if len(name) - k < 2:
            continue
        try:
            _, rows = flk_search_rows(f"{name[:k]} {name[k:]}",
                                      search_type=1, search_range=1,
                                      page_size=10)
        except Exception:
            continue
        for r in rows:
            _consider(row_to_result(r)["title"])
    if not cands:
        return None
    dmin = min(cands.values())
    winners = [t for t, d in cands.items() if d == dmin]
    if len(winners) != 1:
        return None
    title = winners[0]
    short = title[len("中华人民共和国"):] \
        if title.startswith("中华人民共和国") else title
    form = short if _edit_distance(name, short) \
        <= _edit_distance(name, title) else title
    return title, form


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
    corrected_from = None
    if not alias_hit and not _lookup_resolvable(lookup_name):
        # 三路既有解析全部落空才启用近名纠错（843b）：能精确解析的
        # 名称行为零变化；条号不存在等其他失败不经过这里。
        kind, payload = _near_name_match(lookup_name)
        if kind == "none":
            # 867：本地法名全集只覆盖快照＋枚举＋司法部索引（千部
            # 级），全集外的现行法（如湿地保护法）近名纠错无候选。
            # 用 flk 官方检索兜底取候选，仍按同一编辑距离门槛在
            # 本地验核才采纳——检索结果不直接作数。
            fb = _flk_fallback_correction(lookup_name)
            if fb is not None:
                kind, payload = "corrected", fb
        if kind == "corrected":
            canonical, _form = payload
            notes.append(f"输入「{law_name}」已按近名纠正为《{canonical}》"
                         "（近名纠错仅 lookup 提供；核验类工具对写错的"
                         "法名不纠错、只报错）")
            corrected_from = law_name
            lookup_name = canonical
        elif kind == "tie":
            cands = "》、《".join(payload)
            raise SourceError(
                f"「{law_name}」与多部法规的名称都只差一两字，无法确定"
                f"指哪一部：候选《{cands}》。下一步：请用法规全名"
                "重新查询。")
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
                      "parsed_article": heading, "parse_via": via,
                      "name_corrected_from": corrected_from},
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
                  "parsed_article": None, "parse_via": via,
                  "name_corrected_from": corrected_from},
        "machine_conclusion": conclusion,
        "human_review_checklist": list(_HUMAN_REVIEW_BASE),
        "notes": notes,
        "data_snapshot": out["data_snapshot"],
    }


# ---------------------------------------------------------------------------
# check_support：蕴含初筛（零依赖启发式，明示不可替代人工判断）
# ---------------------------------------------------------------------------

_SUPPORT_DISCLAIMER = ("本结论由零依赖词面启发式生成（关键词覆盖、逐分句核对、"
                       "数字一致性、否定极性冲突、排他性限定、条件偷换、"
                       "单位偷换、主体偷换、数量边界方向九类信号），"
                       "只是初筛，不是语义判定，不能替代人工判断；"
                       "引注是否真正支持论断，请结合条文全文与案件事实"
                       "人工复核。")

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


def _clean_for_numbers(text):
    """抽数字前的文本清洗（_extract_numbers 与 _extract_quantities 共用）。

    867 两处实测修正：①「双倍」无数字前缀，旧正则抽不到——先改写
    成「2倍」；②「一次」在「接受一次全面检查」里是惯用语不是数量，
    旧实现抽出 (1, 次) 与引文数字撞出假冲突（865 support-015）——
    非「第/数词＋一次」的惯用「一次」先掩码（第一次、十一次不受
    影响，掩码条件用负向后顾排除）。
    """
    cleaned = _ORDINAL_PARTY_RE.sub(
        "某方", _ENUM_MARK_RE.sub("", text or ""))
    cleaned = cleaned.replace("双倍", "2倍")
    cleaned = re.sub(r"(?<![0-9第一十百千两])一次", "某次", cleaned)
    # 「百分之七十」旧正则只抽到裸值 70（无单位），与引文的
    # 「百分之二十」对不上时只能按附加数字降级——859 对抗集 u05
    # 实测应为硬冲突。先改写成「70%」让单位参与比对。
    cleaned = re.sub(r"百分之([0-9]+(?:\.[0-9]+)?|[零一二三四五六七八九十百千]+)",
                     r"\1%", cleaned)
    cleaned = re.sub(r"千分之([0-9]+(?:\.[0-9]+)?|[零一二三四五六七八九十百千]+)",
                     r"\1‰", cleaned)
    return cleaned


def _extract_numbers(text):
    out = []
    cleaned = _clean_for_numbers(text)
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
    cleaned = _clean_for_numbers(text)
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


def _number_bindings(text):
    """抽（数值，归一单位）→ 左侧名词集合（数字前最近 2 个汉字，
    跳过标点）。用于信号 1i：同一名词在两边绑了不同数字＝数字
    所指被对调（如「价款三倍／损失十倍」对「价款十倍／损失三倍」）。
    只认左侧名词：数字所指的基数词（价款／损失／货值金额…）在
    中文里通常紧贴数字左侧；右侧名词（「三倍赔偿金」）因基数词
    缺席时本就歧义，不纳入。含功能字（的／之／以／处／为／向／
    在／按／自／从／到／上下内外前后第是）的双字不算名词——
    「处十日」「的三年」这类动词＋虚词组合不是所指名词。
    """
    _FUNC_CHARS = set("的之以处为向在按自从到上下内外前后第是")
    out: dict[tuple, set] = {}
    cleaned = _clean_for_numbers(text)
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
        head = cleaned[max(0, m.start() - 6):m.start()]
        cjk = [ch for ch in head if "\u4e00" <= ch <= "\u9fff"]
        left = "".join(cjk[-2:]) if len(cjk) >= 2 else ""
        if left and not (set(left) & _FUNC_CHARS):
            out.setdefault((value, unit), set()).add(left)
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
    # 867 增补：「没有法律、行政法规依据的」是整类禁止规范的
    # 前提，论断抽掉它作绝对化表述即偷换（support-044）。
    r"没有[^，。；]{0,14}依据",
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
                      "仲裁", "抵扣",
                      # 867 增补：处罚类附加是 865 实测的主要漏检形态
                      "罚金", "行政处罚", "行政拘留", "没收", "吊销",
                      "停产整顿",
                      # 867 增补：效力类附加（support-037「与生效判决
                      # 同等的强制执行力」引文里根本没有）
                      "强制执行力", "同等效力", "法律效力")
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


_CLAUSE_SPLIT_RE = re.compile(r"[，。；]")


def _clause_coverages(claim, cite_bigrams):
    """逐分句核对（v4 严格口径）：把论断按，。；切分句，逐句算内容
    二元组在引文中的覆盖率。实质分句＝内容二元组 ≥4 个的句子；
    更短的分句多为连接语或短语，不计入。返回 [(分句预览, 覆盖率,
    二元组数)]。用途：supports 的肯定依据须逐分句成立——整体重合
    高但某实质分句无依据时，整体覆盖会把「只覆盖一半的论断」
    误判成支持（867 正式集的集中失败形态）。"""
    out = []
    for seg in _CLAUSE_SPLIT_RE.split(claim or ""):
        seg = seg.strip()
        if not seg:
            continue
        grams = _content_bigrams(seg)
        if len(grams) < 4:
            continue
        cov = len(grams & cite_bigrams) / len(grams)
        out.append((seg[:24], round(cov, 4), len(grams)))
    return out


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
    downgrades = []
    # 867：问句式 claim 不是论断，无从谈支持与否——865 金标 10 道
    # 问句式 claim（「…是多少？」「如何…？」）答案一律
    # cannot_determine，旧实现却按词面重合判了支持/冲突。问句在
    # 判定末尾统一收口（硬冲突信号仍优先，见结尾覆写条件）。
    is_question = bool(
        re.search(r"[？?]\s*$", claim)
        or re.search(r"(?:多少|如何|怎样|怎么|能否|可否|吗[？?])", claim))
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
        # 867 三分：缺失数字先分「替换/虚构标准」与「附加」——
        # 引文里有同单位数字而论断的对不上（五年 vs 三年、5–10倍
        # vs 15–30倍）是把数字改了；倍/% 在引文里根本没有对应
        # 族（双倍补偿、三倍工资）是虚构标准——两者都是硬冲突。
        # 单位在引文中无同类（如另加十万元、停产六个月）才是
        # 附加细节：覆盖高时降 partial，覆盖不足维持原口径。
        cite_units = {u for _v, u in cite_nums if u}
        sub_nums = [(v, u) for v, u in missing_nums
                    if (u and u in cite_units) or u in ("倍", "%")]
        add_nums = [(v, u) for v, u in missing_nums
                    if (v, u) not in sub_nums]
        if sub_nums:
            desc = "、".join(f"{int(v) if v == int(v) else v}{u}"
                             for v, u in sub_nums)
            conflicts.append(f"数字不一致：论断中的 {desc} 未在引文中"
                             "出现，而引文有同单位的其他数字——疑似"
                             "把引文数字改了或虚构了倍数/比例标准")
        desc = "、".join(f"{int(v) if v == int(v) else v}{u}"
                         for v, u in add_nums)
        if add_nums and coverage >= 0.5:
            downgrades.append(f"论断中的数字 {desc} 未在引文中出现，"
                              "主体内容有词面依据，但该数字部分无引文"
                              "依据，请人工核对")
        elif add_nums and (coverage >= 0.2 or len(add_nums) >= 2):
            # 判冲突的门槛：词面有基本重合，或论断一次塞进 ≥2 个
            # 引文没有的数字（短论断塞两个编造数字是典型造假形态，
            # 覆盖率会因论断太短而偏低，不能只看覆盖率——859 实测
            # 两套金标各有一例卡在这条边界的两侧，按此复合门槛分开）
            conflicts.append(f"论断中的数字 {desc} 未在引文中出现，"
                             "数字依据可能不存在或张冠李戴")
        elif add_nums:
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

    # 信号 1g（867 新增）：分档数字锚定——论断的数字在引文里找得到，
    # 却不在与论断近乎逐字对应的那个分档分句里（support-030：二级
    # 警员至一级警司这一档是三年，论断把另一档的四年安了进来）。
    if claim_nums and not conflicts and claim_bigrams:
        claim_num_set = set(claim_nums)
        if claim_num_set and claim_num_set <= set(cite_nums):
            for seg in re.split(r"[；;。]", citation_text or ""):
                grams = _content_bigrams(seg)
                if not grams:
                    continue
                if len(claim_bigrams & grams) >= 0.8 * len(claim_bigrams):
                    seg_nums = {(v, _norm_unit(u))
                                for v, u in _extract_numbers(seg)}
                    if not claim_num_set <= seg_nums:
                        conflicts.append(
                            "分档数字错位：论断的数字与引文中对应"
                            "分档分句里的数字不一致（引对了条、"
                            "安错了档）")
                    break

    # 信号 1h（867 新增）：分档条件丢失——引文有 ≥2 个量刑/处罚
    # 分档时，论断近乎逐字摘了其中一档的量刑，却丢了该档的适用
    # 条件（如把「肇事后逃逸」档的刑期说成一般情形，859 对抗集
    # b02 形态）。
    tier_segs = [seg for seg in re.split(r"[；;]", citation_text or "")
                 if any(w in seg for w in ("有期徒刑", "拘役", "罚款",
                                           "处罚"))]
    if len(tier_segs) >= 2 and claim_bigrams:
        scored_tiers = sorted(
            ((len(claim_bigrams & _content_bigrams(seg)), seg)
             for seg in tier_segs), key=lambda x: -x[0])
        t_score, t_seg = scored_tiers[0]
        if t_score >= 0.6 * len(claim_bigrams):
            head_grams = _content_bigrams(re.split(r"[，,]", t_seg)[0])
            if len(head_grams - claim_bigrams) >= 3:
                conflicts.append(
                    "分档条件丢失：论断摘了引文某一档的量刑/处罚，"
                    "却丢了该档的适用条件（引文该档以「"
                    + re.split(r"[，,]", t_seg)[0][:20]
                    + "」为前提），把加重档说成了一般情形")

    # 信号 1i（v4 新增）：数字所指对调——论断的数字在引文里都找得到，
    # 但绑的名词被对调（如「价款三倍／损失十倍」对引文「价款十倍／
    # 损失三倍」）。值＋单位比对看不见这种错位：同一名词在引文里
    # 绑着另一个同单位数字、而论断把它绑到本数字上时，即为硬冲突。
    # 只认「同一名词＋不同数字」的对调，不认单纯左侧名词不同
    # （转述改写常换措辞，单侧不同不判）。
    if claim_nums and not conflicts and coverage >= 0.2:
        claim_bind = _number_bindings(claim)
        cite_bind = _number_bindings(citation_text)
        cite_noun_values: dict[tuple, set] = {}
        for (cv, cu), nouns in cite_bind.items():
            for noun in nouns:
                cite_noun_values.setdefault((noun, cu), set()).add(cv)
        for (value, unit), nouns in claim_bind.items():
            if (value, unit) not in set(cite_nums):
                continue
            for noun in nouns:
                # 引文侧该名词只绑一个同单位数字时才判对调——
                # 多档条文里同一泛词（如「的处」）本就绑多个数字，
                # 非单值绑定不算对调（v03／g03 误报根因）。
                cite_vals = cite_noun_values.get((noun, unit), set())
                if len(cite_vals) != 1:
                    continue
                others = cite_vals - {value}
                if others:
                    conflicts.append(
                        f"数字所指对调：论断把「{noun}」绑到"
                        f"{int(value) if value == int(value) else value}"
                        f"{unit}，引文里「{noun}」却是"
                        f"{'、'.join(str(int(v)) if v == int(v) else str(v) for v in sorted(others))}"
                        f"{unit}——数字本身都在引文里，但所指被换了")
                    break
            if conflicts:
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
            and not any(w in claim for w in
                        ("不得", "不能", "禁止", "不准", "不应")) \
            and coverage >= 0.25:
        # 867：旧实现用 claim_neg 一票否决——论断把引文的条件句
        # （「没有法律、行政法规依据的」）原样抄进来时，「没有」
        # 会把真正的极性反转（不得减损→可以减损，support-043）
        # 一并挡掉。改看论断有没有对主行为的禁止表述。
        # v4（2026-11-26）：引文里另有禁止表述时才算疑似冲突——
        # 若论断的每一个许可表述（可以/有权＋后续动作）在引文里
        # 同样以许可形式出现（如食安法148条「不得推诿」是另一句
        # 对经营者的义务、而「可以向经营者要求赔偿损失」引文原样
        # 许可），就不是极性反转，不判冲突；任一许可未被引文许
        # 可时仍判冲突。
        _permit_affirmed = True
        for _pm in _PERMIT_RE.finditer(claim):
            _m_tail = re.match(r"[^，。；]{2,12}", claim[_pm.end():])
            _tail = re.sub(r"[^一-鿿A-Za-z0-9]", "",
                           _m_tail.group(0)) if _m_tail else ""
            if len(_tail) < 2 or not re.search(
                    r"(?:可以|有权)" + re.escape(_tail), citation_text):
                _permit_affirmed = False
                break
        if not _permit_affirmed:
            conflicts.append("极性疑似冲突：引文含禁止表述（不得/禁止/不准），"
                             "论断却表述为可以/有权")
    elif _PROHIBIT_RE.search(claim) and coverage >= 0.25 and any(
            not re.search(r"(不|未|没有|无)$",
                          citation_text[max(0, pm.start() - 4):pm.start()])
            and any(a in claim and a in citation_text
                    for a in ("请求", "返还", "解除", "终止", "追偿",
                              "主张", "赔偿", "授权"))
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
    # 867：引文是列举条（一）（二）（三）…时，若论断被其中某一项
    # 高度覆盖（≥85%），条件只在该项内查——旧实现拿全条的条件
    # 扫论断，转述第（一）项的论断会被第（三）项的条件误判偷换
    # （865 support-010 实测）。
    cond_text = citation_text
    enum_split = re.split(
        r"(?=[（(][一二三四五六七八九十]+[）)])", citation_text or "")
    enum_parts = [seg for seg in enum_split if seg.strip()]
    if len(enum_parts) >= 2 and claim_bigrams:
        # 列举条的引言（「禁止下列行为：」）是各款的共同语境，
        # 拼回每一款再算覆盖，否则引言里的词会把覆盖率拉低到
        # 门槛之下、作用域限定落空（865 support-010/012 实测）。
        lead = enum_split[0] if enum_split and \
            not re.match(r"[（(][一二三四五六七八九十]+[）)]",
                         enum_split[0].strip()) else ""
        scoped = [lead + seg for seg in enum_parts]
        seg_scores = sorted(
            ((len(claim_bigrams & _content_bigrams(seg)), seg)
             for seg in scoped), key=lambda x: -x[0])
        # 分母只算论断里有引文依据的那部分词——论断另加的处罚
        # 分句（如另加罚款幅度）不该把作用域限定冲掉（support-012）。
        supported = len(claim_bigrams & cite_bigrams)
        if supported and seg_scores[0][0] >= 0.8 * supported:
            cond_text = seg_scores[0][1]
    cite_conds = [p for p in _CONDITION_CITE_RES
                  if p.search(cond_text)]
    # 867 修正：「没有…依据」前提只管辖它所在的那句禁止规范——
    # 论断根本没转述那句（只讲了同条别句）时不算丢条件
    # （support-045 与 044 的分界：044 转述了禁止规范本身）。
    if cite_conds and all(p.pattern.startswith("没有")
                           for p in cite_conds) \
            and not any(w in claim for w in
                        ("减损", "增加其义务", "设置市场准入", "干预",
                         "不得")):
        cite_conds = []
    claim_keeps_condition = (
        any(w in claim for w in _CONDITION_CLAIM_WORDS)
        or any(p.search(claim) for p in _CONDITION_CLAIM_RES))
    missing_conds = [p for p in cite_conds if not p.search(claim)]
    weak_dropped = any(p.search(cond_text)
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

    # 信号 8（867 新增）：整分句附加编造——论断某个分句带处罚/责任
    # 类实质内容、与引文词面几乎零重合，是把引文没有的后果整段
    # 塞进来（865 实测「并处罚金」「直接行政处罚权」类无信号漏检）。
    # 一处只降级，两处以上判硬冲突。
    _FABRICATED_MARKERS = ("处罚", "罚金", "赔偿", "拘留", "没收",
                           "吊销", "停产", "整顿", "补偿", "责任")
    fabricated = []
    for clause in re.split(r"[，。；;]", claim):
        clause = clause.strip()
        if len(clause) < 6 or not any(
                w in clause for w in _FABRICATED_MARKERS):
            continue
        grams = _content_bigrams(clause)
        if len(grams) >= 4 and \
                len(grams & cite_bigrams) < 0.15 * len(grams):
            fabricated.append(clause)
    if len(fabricated) >= 2:
        conflicts.append("附加编造：论断多处分句带有处罚/责任类实质"
                         "内容、与引文词面几乎无重合（"
                         + "；".join(c[:20] for c in fabricated[:2])
                         + "…），该部分无引文依据")
    elif fabricated:
        downgrades.append("论断分句「" + fabricated[0][:24]
                          + "」带实质内容但与引文词面几乎无重合，"
                          "该部分可能无引文依据，请人工核对")
        signals.append(downgrades[-1])

    # 信号 9（867 新增）：要件显式否定——论断用「无论是否/不管
    # 是否＋引文中的要件」把构成要件整体抹掉（865 support-005）。
    m_req = re.search(r"(?:无论|不管)是否(.{2,12}?)(?:[，。；]|都|就|$)",
                      claim)
    if m_req:
        elem = re.sub(r"[是否都就]", "", m_req.group(1))
        # 要件短语在引文里常被其他成分隔开（如「造成重要军事通信
        # 中断」），整串子串匹配会落空——按「或者/和」拆段，任一
        # 段在引文中出现即认定要件确实存在于引文（support-005）。
        parts = [p for p in re.split(r"或者|和|及|与", elem)
                 if len(p) >= 2]
        if len(elem) >= 4 and (elem in citation_text
                               or any(p in citation_text for p in parts)):
            conflicts.append(f"要件否定偷换：论断称「无论是否{elem}」"
                             f"都成立，引文却把「{elem}」列为要件，"
                             "论断把成立条件整体抹掉了")

    # 信号 10（867 新增）：绝对化推断——「只要…必然/一律」式无条件
    # 结论，而引文规则带构成/后果类条件且论断未保留（support-025：
    # 引文明明分「构成犯罪/尚不构成犯罪」两档，论断却说只要检出
    # 就必然追究刑事责任）。绝对化词面明确、条件丢失可核，判硬冲突。
    if re.search(r"只要[^。；]{2,24}?(?:必然|一律|都构成|都会|都将被)",
                 claim) \
            and coverage >= 0.3 \
            and any(w in citation_text for w in
                    ("构成犯罪", "致使", "情节严重", "后果严重")) \
            and not any(w in claim for w in
                        ("构成犯罪", "致使", "情节严重", "后果严重")):
        conflicts.append("绝对化推断：论断用「只要…必然/一律」把"
                         "结论说成无条件成立，引文规则却带有构成/"
                         "后果类条件（如是否构成犯罪），论断把条件"
                         "丢了、把或然说成了必然")

    # 信号 11（867 新增）：三式窄冲突——主管部门对调、鼓励说成
    # 禁止、「不再追究」与引文「依法追究」正相反。均为成对表述
    # 各执一词才触发，误伤面窄。
    def _dept_holder(text):
        # 两种语序：「X是…主管部门」与「…的主管部门是X」（865
        # support-014 是后者，旧实现只认前者、整题漏检）。
        m = re.search(r"([^，。；]{2,12}?)是[^，。；]{0,8}主管部门", text)
        if m:
            return m.group(1)
        m = re.search(r"主管部门是([^，。；]{2,14})", text)
        return m.group(1) if m else None

    dept_claim = _dept_holder(claim)
    dept_cite = _dept_holder(citation_text)
    if dept_claim and dept_cite and dept_claim != dept_cite \
            and coverage >= 0.2:
        conflicts.append(f"主管部门对调：引文的主管部门是"
                         f"「{dept_cite}」，论断却说成「{dept_claim}」")
    if any(w in claim for w in ("禁止", "不得", "不准")) \
            and "鼓励" in citation_text \
            and not any(w in citation_text for w in ("禁止", "不得", "不准")) \
            and coverage >= 0.2:
        conflicts.append("态度对调：引文对该事项是「鼓励」，论断却"
                         "说成禁止，方向相反")
    if any(w in claim for w in ("不再追究", "不予追究", "不追究")) \
            and "追究" in citation_text \
            and not any(w in citation_text
                        for w in ("不再追究", "不予追究", "不追究")) \
            and coverage >= 0.2:
        conflicts.append("责任结论对调：论断称不再/不予追究，引文却"
                         "规定要追究，结论相反")

    # 信号 12（867 新增，只降级）：范围扩张——论断称某对象「也适用」
    # 本条，而该对象短语在引文里根本没有（support-032 把一级警督
    # 以上扩进只管二级警督以下的条）。
    m_scope = re.search(
        r"(.{4,30}?)(?:也适用|也必须适用|同样适用|亦适用)", claim)
    if m_scope:
        stem = m_scope.group(1)
        # 主语可能在句首（外国公民…也必须适用）也可能贴着动词
        # （一级警督以上的人民警察也适用），首尾都查。
        ends = [stem[:8], stem[max(0, len(stem) - 8):]]
        if all(e and e not in citation_text for e in ends):
            downgrades.append("论断把适用范围扩到「" + stem[:16]
                              + "」，该对象未见于引文，请人工核对范围")
            signals.append(downgrades[-1])

    # 信号 12b（867 新增）：一律定额化——引文给的是上限量刑
    # （五日以下拘留），论断用「一律处五日拘留」说成无档次的
    # 定额（support-047）。
    if "一律" in claim \
            and re.search(r"处[^，。；]{0,10}(拘留|罚款|有期徒刑|管制|拘役)",
                          claim) \
            and re.search(r"(以下|不超过)[^，。；]{0,4}"
                          r"(拘留|罚款|有期徒刑|管制|拘役)",
                          citation_text):
        conflicts.append("量刑定额化：引文规定的是上限量刑（如"
                         "「…以下」），论断用「一律处…」说成没有"
                         "裁量档次的定额，处罚幅度被改动")

    # 信号 13（867 新增）：定义范围相反——引文把概念界定在某范围
    # 「以外」，论断却说成「包括」该范围内的对象（support-053）。
    if re.search(r"所称[^，。；]{1,20}，?包括", claim) \
            and re.search(r"所称[^，。；]{1,20}，?是指[^，。；]{1,40}以外",
                          citation_text):
        conflicts.append("定义范围相反：引文把该概念界定在相应范围"
                         "「以外」，论断却把范围内的对象说成包括在内，"
                         "定义被反转")

    # 信号 14（867 新增，只降级）：列举条跨项拼接——论断某个分句
    # 与引文某一项高度对应，却掺入了另一项的内容（support-042：
    # 把副领事的三等秘书衔安给了领事随员）。
    if len(enum_parts) >= 3 and claim_bigrams:
        # 只认真正的款项段（以（一）等序号开头）；引言是各款共同
        # 语境，其词不算「从其他款借来」（support-010 实测误报）。
        items_only = [seg for seg in enum_parts
                      if re.match(r"\s*[（(][一二三四五六七八九十]+[）)]",
                                  seg)]
        lead_grams = _content_bigrams(enum_split[0]) \
            if enum_split else set()
        item_grams = [_content_bigrams(
            re.sub(r"[（(][一二三四五六七八九十]+[）)]", "", seg))
            for seg in items_only]
        # 款项主体（如「领事随员」）：分句点名了主体就锚定该款，
        # 不按词面重合猜（重合最高的是被借内容的那一款时，
        # 按重合锚定会把借用关系整个看反）。
        item_heads = []
        for seg in items_only:
            head = re.split(r"[：:]", seg, maxsplit=1)[0]
            head = re.sub(r"[（(][一二三四五六七八九十]+[）)]", "", head)
            head = re.sub(r"[^一-鿿A-Za-z0-9]", "", head)
            item_heads.append(head)
        for clause in re.split(r"[，。；;、]", claim):
            grams = _content_bigrams(clause)
            if len(grams) < 6 or not item_grams:
                continue
            best = None
            # 主体名按长优先锚定：「领事」是「副领事」的子串，
            # 先匹配短名会锚错款。
            for i in sorted(range(len(item_heads)),
                            key=lambda k: -len(item_heads[k])):
                head = item_heads[i]
                if len(head) >= 2 and head in clause:
                    best = i
                    break
            if best is None:
                overlaps = [len(grams & ig) for ig in item_grams]
                best = max(range(len(item_grams)),
                           key=lambda i: overlaps[i])
                if overlaps[best] < 0.35 * len(grams):
                    continue
            extra = grams - item_grams[best] - lead_grams
            borrowed = sum(1 for g in extra
                           if any(g in item_grams[i]
                                  for i in range(len(item_grams))
                                  if i != best))
            # 锚定款自身的值词大段缺失又借了别款的值，是把对应
            # 关系改了（判冲突）；只借不缺是掺入（降级）。
            head_grams = _content_bigrams(item_heads[best])
            own_missing = (item_grams[best] - head_grams - lead_grams) \
                - grams
            if borrowed >= 3 and len(own_missing) >= 2:
                conflicts.append("款项对应错位：论断分句「"
                                 + clause.strip()[:24]
                                 + "」与引文该款的对应内容对不上"
                                 "（掺入了其他款项的内容、又丢了"
                                 "本款的内容）")
                break
            if borrowed >= 3:
                downgrades.append("论断分句「" + clause.strip()[:24]
                                  + "」掺入了引文其他款项的内容"
                                  "（跨项拼接），请人工核对每项的"
                                  "对应关系")
                signals.append(downgrades[-1])
                break

    # 信号 14b（867 新增）：对应关系清单的令牌级核对——引文是
    # 「主体：值、值」式对应表时，把论断对某主体断言的值集合与
    # 引文该主体的值集合逐令牌比：断言了别款的值（借）且漏了
    # 本款的值（缺）→ 对应关系被改（冲突）；只借不缺 → 降级。
    # bigram 层面「二等秘书衔」与「三等秘书衔」只差一个二元组，
    # 词面信号看不见，必须令牌级比（support-040/042）。
    corr_items = {}
    for seg in enum_parts:
        m_corr = re.match(
            r"\s*[（(][一二三四五六七八九十]+[）)]\s*([^：:]{2,10})[：:]"
            r"(.+)", seg, re.S)
        if m_corr:
            vals = {v.strip(" ；;。")
                    for v in re.split(r"[、，；;]", m_corr.group(2))
                    if len(v.strip(" ；;。")) >= 3}
            if vals:
                corr_items[m_corr.group(1).strip()] = vals
    if len(corr_items) >= 3:
        all_vals = set().union(*corr_items.values())
        for clause in re.split(r"[，。；;]", claim):
            head_hit = None
            for head in sorted(corr_items, key=len, reverse=True):
                if head in clause:
                    head_hit = head
                    break
            if head_hit is None:
                continue
            if not any(w in clause for w in ("对应", "是", "为")):
                continue
            # 断言值按已知值词表在分句里直接匹配——「…对应的外交
            # 衔级是二等秘书衔」这类句子按标点切分会把修饰语粘在
            # 值上，词表匹配才干净。
            asserted = {v for v in all_vals if v in clause}
            own = corr_items[head_hit]
            borrowed_t = {v for v in asserted - own if v in all_vals}
            missing_t = own - asserted
            if borrowed_t and missing_t:
                conflicts.append(
                    f"对应关系错位：「{head_hit}」在引文中对应"
                    f"「{'、'.join(sorted(own))}」，论断却说成"
                    f"「{'、'.join(sorted(asserted))}」——借了别款的"
                    "值、漏了本款的值")
                break
            if borrowed_t:
                downgrades.append(
                    f"「{head_hit}」的对应值里掺入了别款的值"
                    f"（{'、'.join(sorted(borrowed_t))}），请人工"
                    "核对对应关系")
                signals.append(downgrades[-1])
                break

    # 信号 15（867 新增）：计入与否反转——论断说「不计入」，引文
    # 明明「计算在…内/计入」（support-031）。
    if "不计入" in claim and "不计入" not in citation_text \
            and ("计算在" in citation_text or "计入" in citation_text) \
            and coverage >= 0.2:
        conflicts.append("计入与否相反：论断称不计入，引文却规定"
                         "计算在内/计入，结论被反转")

    # 信号 16（867 新增）：程序条件被明文否定——论断称「不需要/
    # 无条件」＋某程序，引文却以「经该程序」为前提（support-033
    # 「不需要经过考核」vs 引文「经考核具备晋级条件的」）。
    m_proc = re.search(
        r"(?:无条件|不需要|无需|不必|不用)[^，。；]{0,6}"
        r"(考核|审查|批准|同意|许可|登记)", claim)
    if m_proc and ("经" + m_proc.group(1) in citation_text
                   or "经过" + m_proc.group(1) in citation_text) \
            and coverage >= 0.2:
        conflicts.append(f"程序条件被否定：论断称不需要{m_proc.group(1)}，"
                         f"引文却以「经{m_proc.group(1)}」为前提，"
                         "论断把必经程序抹掉了")
    # 同族变体：「无需取得X」而引文明定应当取得X（support-049
    # 测绘资质证书）——被否定的对象在引文里以「取得X」出现即冲突。
    m_get = re.search(
        r"(?:无需|不需要|不必|不用)[^，。；]{0,6}取得([^，。；]{2,12})",
        claim)
    get_obj = re.split(r"即可|就可|方可|后",
                       m_get.group(1))[0] if m_get else ""
    if m_get and len(get_obj) >= 2 \
            and re.search(r"取得[^，。；]{0,10}" + re.escape(get_obj),
                          citation_text) \
            and coverage >= 0.2:
        conflicts.append(f"取得要求被否定：论断称无需取得"
                         f"「{get_obj}」，引文却明定应当取得，"
                         "结论相反")

    # 信号 17（867 新增）：损失自担反转——引文规定「应当赔偿」，
    # 论断却让受损方「自行承担」（support-055）。
    if any(w in claim for w in ("自行承担", "自己承担")) \
            and "应当赔偿" in citation_text and coverage >= 0.2:
        conflicts.append("损失承担反转：引文规定造成损失应当赔偿，"
                         "论断却让受损方自行承担，责任归属被反转")

    # 排他对齐（859）：论断用「只能」类排他表述、引文本身也有排他
    # 标记（含「非…不可」句式）时，两侧的排他语义是对齐的——这类
    # 忠实转述常因措辞不同词面覆盖偏低，覆盖 ≥0.4 且无其他降级
    # 信号时按支持处理，不再一律压成「部分支持」。
    exclusive_aligned = (
        any(w in claim for w in _EXCLUSIVE_CLAIM_WORDS)
        and (any(w in citation_text for w in _EXCLUSIVE_CITE_WORDS)
             or _EXCLUSIVE_CITE_RE.search(citation_text)))
    # v4 严格口径（2026-11-26）：支持须有逐分句的肯定依据——
    # 867 正式集的失败形态集中在「整体词面重合够高、无硬冲突信号，
    # 但论断的实质分句并未被引文覆盖」却被判 supports（partial
    # 11 题中 10 题、unsupported 29 题中 11 题被判成 supports，
    # 见 RESULT_867.md 混淆分布）。故 supports 增设逐分句门：
    # 全部实质分句覆盖 ≥0.5；有分句未覆盖时降为 partial；整体
    # 重合 ≥0.28 但无任何实质分句被覆盖时（弥散重合）不再给
    # partial，落 cannot_determine。
    clause_cov = _clause_coverages(claim, cite_bigrams)
    all_clauses_covered = bool(clause_cov) and all(
        c >= 0.5 for _seg, c, _n in clause_cov)
    any_clause_covered = any(
        c >= 0.5 for _seg, c, _n in clause_cov)
    if conflicts:
        verdict = "unsupported"
        verdict_zh = "初筛：不支持（检出硬冲突信号，见 reasons）"
    elif exclusive_aligned and coverage >= 0.4 and not downgrades:
        # 排他对齐分支不设逐分句门：排他对齐本身就是肯定依据——
        # 「只能…」对「非…不可」的忠实转述常因措辞不同致分句
        # 覆盖偏低（a859-e01：分句「只能采用书面形式」对引文
        # 「非订立书面劳动合同不可」仅 0.14），逐分句门会把它
        # 误压成 partial；排他标记两侧对齐＋整体覆盖 ≥0.4＋无
        # 降级信号已是充分的肯定证据。
        verdict = "supports"
        verdict_zh = ("初筛：支持（论断与引文的排他性表述两侧对齐，"
                      "词面部分重合）")
    elif coverage >= 0.5 and downgrades:
        verdict = "partial"
        verdict_zh = ("初筛：部分支持（主体内容有词面依据，但有附加/全称"
                      "表述超出引文范围，见 reasons）")
    elif coverage >= 0.5 and all_clauses_covered:
        verdict = "supports"
        verdict_zh = "初筛：支持（词面高度重合、逐分句均有依据且无冲突信号）"
    elif coverage >= 0.5:
        verdict = "partial"
        verdict_zh = ("初筛：部分支持（整体词面重合较高，但论断中有"
                      "分句在引文中无依据，见 clause_coverage）")
    elif coverage >= 0.28 and any_clause_covered:
        verdict = "partial"
        verdict_zh = "初筛：部分支持（词面部分重合，可能只覆盖论断的一部分）"
    elif coverage >= 0.28:
        verdict = "cannot_determine"
        verdict_zh = ("初筛：无法判断（整体词面有重合，但论断中没有"
                      "任何实质分句被引文覆盖，启发式无从判定）")
    else:
        verdict = "cannot_determine"
        verdict_zh = "初筛：无法判断（词面重合过低，启发式无从判定）"
    if verdict == "cannot_determine" and downgrades and coverage >= 0.2:
        # 已检出具体的超范围信号（全称/附加内容）时，不许只说
        # 「无法判断」——降为部分支持并把信号亮出来（859）。
        verdict = "partial"
        verdict_zh = ("初筛：部分支持（词面重合有限，且有表述超出"
                      "引文范围，见 reasons）")
    if is_question and not conflicts:
        # 867：问句不是论断（见函数开头 is_question）——无硬冲突时
        # 一律无法判断，不许按词面重合判支持/部分支持。
        verdict = "cannot_determine"
        verdict_zh = ("初筛：无法判断（输入是问句而非论断，支持关系"
                      "无从判定；如需判断请先把问句改写成陈述句）")
    return {
        "schema": RESULT_SCHEMA, "tool": "check_support",
        "method": "lexical_heuristic_v4.0",
        "claim": claim, "citation": citation_meta or {"text_provided": True},
        "citation_text": citation_text,
        "verdict": verdict, "verdict_zh": verdict_zh,
        "reasons": conflicts + signals,
        "coverage": round(coverage, 4),
        "clause_coverage": [
            {"clause": seg, "coverage": c, "bigrams": n}
            for seg, c, n in clause_cov],
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
                       "只认带《》书名号的引注：不带书名号裸写的「法名第N条」"
                       "不会被抽取核验（明示边界，见 README「已知盲区」）。"
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
                       "某个论断。方法为零依赖词面启发式 v4.0（关键词覆盖、逐分句核对、"
                       "数字一致性、否定极性冲突、排他性限定、条件偷换、单位偷换、"
                       "主体偷换、数量边界方向、多档分档锚定、数字所指对调），"
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
