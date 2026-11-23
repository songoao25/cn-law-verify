#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify — 中国法律法规条文核验 MCP server（P0）。

定位：给 AI 的法条核验层。只做两件事——从官方源（国家法律法规数据库
flk.npc.gov.cn）实时取回法规与条文原文，并对给定引文做代码层核验
（存在性 / 逐字比对 / 效力状态三检）。核验逻辑是确定性代码，不经模型。

设计纪律（继承 flk-mcp 的经验、补其缺点，详见 docs/DESIGN.md）：
- 数据只来自官方源，请求只发往白名单域名，且只走 HTTPS；
- 下载条文时全程开启 TLS 证书校验，跟随重定向逐跳复检目标域名
  （flk-mcp 在此处关掉过证书校验，本项目将其作为回归红线修复）；
- 单次查询不落盘、不缓存、不建本地库；发往官方源的只有本次检索词本身；
- 遇验证码/拦截页立即终止并如实报错，绝不用旧数据冒充实时结果；
- 目标域名不可经环境变量改写（环境变量只许调超时）。

零依赖：纯 Python 3.10+ 标准库，手写 MCP stdio JSON-RPC 最小闭环。
协议版本策略：按 2025-03-26 / 2025-11-25 的会话式语义实现，
同时兼容 2024-11-05；请求处理保持无状态。

用法：
    python3 cn_law_verify_server.py            # stdio 模式（MCP 客户端拉起）
    python3 cn_law_verify_server.py --version
"""

import difflib
import io
import json
import os
import re
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
SERVER_VERSION = "0.1.0"
RESULT_SCHEMA = "cn-law-verify/result@1"
SUPPORTED_PROTOCOLS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
DEFAULT_PROTOCOL = "2025-03-26"

FLK_BASE = "https://flk.npc.gov.cn"
# API 请求只许发往这两个官方域名；下载跳转另许联通云对象存储域名后缀
# （flk 官方 DOCX 的签名下载地址所在域名），且逐跳复检。
API_HOSTS = {"flk.npc.gov.cn", "wb.flk.npc.gov.cn"}
DOWNLOAD_HOST_SUFFIXES = (".cucloud.cn",)
USER_AGENT = "cn-law-verify/0.1.0 (+https://github.com/songoao25/cn-law-verify)"

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


def _open_once(url, data=None, headers=None, *, for_download=False):
    _check_url(url, for_download=for_download)
    req = urllib.request.Request(url, data=data,
                                 method="POST" if data is not None else "GET")
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Referer", "https://flk.npc.gov.cn/index.html")
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
             body_limit=None, retries=5):
    """发请求并读完响应体；3xx 时逐跳复检域名后跟随；失败重试。"""
    last_err = None
    for attempt in range(retries):
        try:
            current, payload, hdrs = url, data, headers
            for _ in range(MAX_REDIRECTS + 1):
                try:
                    resp = _open_once(current, payload, hdrs,
                                      for_download=for_download)
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
                    return _read_body(
                        resp, limit=body_limit,
                        expected=int(expected) if expected else None)
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
    raw = _request(url, data, headers)
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


def fetch_law_docx_paragraphs(bbbs):
    """下载官方 DOCX 全文并解析为段落文本列表（TLS 全程校验、逐跳复检）。"""
    url = (f"{FLK_BASE}/law-search/download/mobile"
           f"?bbbs={urllib.parse.quote(bbbs)}&format=docx")
    raw = _request(url, for_download=True, body_limit=MAX_DOWNLOAD_BYTES,
                   retries=5)
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


def tool_verify_citation(args):
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
        official_text = None
        text_source = None
        try:
            paragraphs = fetch_law_docx_paragraphs(law["id"])
            found = extract_article(paragraphs, num, sub)
            if found:
                official_text = found["text"]
                text_source = "official_docx_full_text"
        except SourceError as e:
            notes.append(f"官方全文（DOCX）获取失败，转入上下文窗口兜底：{e}")
        if official_text is None:
            windows = fetch_hit_windows(law["id"], heading)
            joined = "\n".join(windows)
            segment = extract_article_from_text(joined, num, sub)
            if segment:
                official_text = segment["text"]
                text_source = "official_context_window"
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


def _stub(name, phase):
    def _impl(_args):
        raise SourceError(f"工具 {name} 尚在开发中（计划 {phase} 落地）；"
                          "当前可用工具：flk_search、verify_citation")
    return _impl


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
        "description": "（开发中，P1）取单部法规元数据＋章节目录＋版本沿革。",
        "inputSchema": {"type": "object",
                        "properties": {"id": {"type": "string"},
                                       "law": {"type": "string"}}},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("get_law", "P1"),
    },
    {
        "name": "get_article",
        "description": "（开发中，P1）取单条条文逐字原文（快照库通道）。",
        "inputSchema": {"type": "object",
                        "properties": {"law": {"type": "string"},
                                       "article": {"type": "string"}},
                        "required": ["law", "article"]},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("get_article", "P1"),
    },
    {
        "name": "search_articles",
        "description": "（开发中，P1）跨法规的条文级全文检索（快照库通道）。",
        "inputSchema": {"type": "object",
                        "properties": {"keywords": {"type": "string"}},
                        "required": ["keywords"]},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("search_articles", "P1"),
    },
    {
        "name": "check_citations_in_text",
        "description": "（开发中，P2）整段文本引注核验：抽取文中全部《法名》第N条"
                       "引用逐条比对，返回计数与问题清单。抽取模块已就绪，工具随 P2 开放。",
        "inputSchema": {"type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"]},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("check_citations_in_text", "P2"),
    },
    {
        "name": "lookup",
        "description": "（开发中，P2）一句话自然语言直达：解析法名＋条号，"
                       "输出「机器结论／人工复核清单」两段式结果。",
        "inputSchema": {"type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"]},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("lookup", "P2"),
    },
    {
        "name": "check_support",
        "description": "（规划中，v2）蕴含核验：判断引注是否真的支持所引论断"
                       "（需调用方自备模型，server 只提供原文片段与判定协议）。",
        "inputSchema": {"type": "object",
                        "properties": {"claim": {"type": "string"},
                                       "citation_id": {"type": "string"}},
                        "required": ["claim", "citation_id"]},
        "annotations": {"readOnlyHint": True},
        "_impl": _stub("check_support", "v2"),
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


def main():
    if "--version" in sys.argv:
        print(f"{SERVER_NAME} {SERVER_VERSION}")
        return 0
    serve_stdio()
    return 0


if __name__ == "__main__":
    sys.exit(main())
