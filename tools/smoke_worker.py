#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLV Worker 生产端点冒烟（可复用版）：https://clv.songsong.win/mcp，
Streamable HTTP，initialize + tools/list + 8 个工具各一次真实调用，只记通过/不通过，不计分。
中性题目：民法典第 577 条（公开法条，不在任何题集/开发集内）。

Bearer 鉴权（Worker 要求 Authorization: Bearer <CLV_API_SECRET>）：
- 唯一来源：环境变量 CLV_API_SECRET（os.environ.get 读取）。
- 脚本不从任何文件、任何连接器、任何历史记录里取密钥。
- 密钥绝不打印到 stdout / stderr / 日志 / 落盘文件；请求头里用可以，
  但任何回显、报错、调试输出在打印前都经过脱敏（_scrub 把密钥值替换为 [REDACTED]）。
- 未提供 CLV_API_SECRET 时：先发一次无鉴权请求确认端点存活与 401，
  再以 "SKIPPED auth-required（未提供 CLV_API_SECRET，冒烟未执行、不计分）"
  退出，退出码 0 表示跳过（区别于失败）。
用法（持密钥者）：CLV_API_SECRET=... python3 tools/smoke_worker.py
  密钥只进当前 shell 进程的环境变量，不落盘。
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

URL = "https://clv.songsong.win/mcp"

SECRET = os.environ.get("CLV_API_SECRET") or None

LAW = "中华人民共和国民法典"
ARTICLE = "第五百七十七条"


def _scrub(text):
    """脱敏：任何待打印文本先过一遍，密钥值（若存在）一律替换为 [REDACTED]。"""
    s = str(text)
    if SECRET and SECRET in s:
        s = s.replace(SECRET, "[REDACTED]")
    return s


def _headers():
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    }
    if SECRET:
        h["Authorization"] = "Bearer " + SECRET
    return h


def _parse_payload(raw):
    """Streamable HTTP 可能返回 SSE（data: {...} 行）或纯 JSON。"""
    payload = None
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if line.startswith("{"):
            try:
                payload = json.loads(line)
                break
            except Exception:
                continue
    if payload is None:
        payload = json.loads(raw)
    return payload


def raw_post(body_dict):
    """发一次 POST，返回 (http_status, payload_or_None, err_str)。
    err_str 已脱敏，不含任何请求头/密钥内容。"""
    body = json.dumps(body_dict).encode("utf-8")
    req = urllib.request.Request(URL, data=body, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", "replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", "replace")
        except Exception:
            raw = ""
        return e.code, None, _scrub(f"HTTP {e.code}: {raw[:200]}")
    except Exception as e:
        return None, None, _scrub(f"http error: {e}")
    try:
        return status, _parse_payload(raw), ""
    except Exception:
        return status, None, _scrub(f"unparsable response: {raw[:300]}")


def rpc(method, params=None, mid=1):
    status, payload, err = raw_post(
        {"jsonrpc": "2.0", "id": mid, "method": method, "params": params or {}})
    if err:
        return None, 0.0, err
    return payload, 0.0, ""


def call_tool(name, args, mid):
    resp, _dt, err = rpc("tools/call", {"name": name, "arguments": args}, mid)
    if err:
        return False, err
    if not isinstance(resp, dict) or "result" not in resp:
        return False, _scrub(str(resp)[:300])
    r = resp["result"]
    if r.get("isError"):
        return False, _scrub(str(r)[:300])
    txt = ""
    for c in r.get("content", []):
        if c.get("type") == "text":
            txt += c.get("text", "")
    try:
        return True, json.loads(txt)
    except Exception:
        return True, txt[:200]


def probe_unauthenticated():
    """无 Bearer 路径：发一次无鉴权 initialize，确认端点存活与 401。
    返回 (status, err_str)。"""
    body = json.dumps({
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                   "clientInfo": {"name": "clv-smoke-probe", "version": "1.0"}},
    }).encode("utf-8")
    req = urllib.request.Request(URL, data=body, headers={
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "clv-smoke-probe/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            resp.read()
            return resp.status, ""
    except urllib.error.HTTPError as e:
        try:
            e.read()
        except Exception:
            pass
        return e.code, ""
    except Exception as e:
        return None, _scrub(f"http error: {e}")


def main():
    if not SECRET:
        status, err = probe_unauthenticated()
        if err:
            print(f"probe: FAIL {err}", flush=True)
            print("SKIPPED auth-required（未提供 CLV_API_SECRET，冒烟未执行、不计分）",
                  flush=True)
            print("提示：端点无鉴权探针未得到预期 401，无法确认端点存活；"
                  "请持密钥者检查端点后重试。", flush=True)
            return 1
        if status == 401:
            print(f"probe: endpoint alive, HTTP 401 unauthorized (no Bearer sent)",
                  flush=True)
            print("SKIPPED auth-required（未提供 CLV_API_SECRET，冒烟未执行、不计分）",
                  flush=True)
            print("跑完整冒烟：CLV_API_SECRET=... python3 tools/smoke_worker.py"
                  "（密钥只进本次 shell 环境变量，不落盘）", flush=True)
            return 0
        print(f"probe: unexpected HTTP {status} (expected 401)", flush=True)
        print("SKIPPED auth-required（未提供 CLV_API_SECRET，冒烟未执行、不计分）",
              flush=True)
        print("提示：端点无鉴权探针未返回 401，请持密钥者核查端点状态后再跑完整冒烟。",
              flush=True)
        return 1

    recs = []

    def log(name, ok, detail=""):
        recs.append({"tool": name, "ok": bool(ok), "detail": _scrub(str(detail))[:400],
                     "at": time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time.localtime())})
        print(f"{name}: {'OK' if ok else 'FAIL'} {_scrub(str(detail))[:160]}", flush=True)

    resp, _dt, err = rpc("initialize", {
        "protocolVersion": "2025-03-26", "capabilities": {},
        "clientInfo": {"name": "clv-worker-smoke", "version": "1.0"}}, 1)
    si = (resp or {}).get("result", {}).get("serverInfo", {}) if isinstance(resp, dict) else {}
    log("initialize", bool(si.get("name")) and bool(si.get("version")), si if si else err)

    resp, _dt, err = rpc("tools/list", {}, 2)
    tools = [t.get("name") for t in (resp or {}).get("result", {}).get("tools", [])] \
        if isinstance(resp, dict) else []
    log("tools/list", len(tools) == 8, tools if tools else err)

    ok, law = call_tool("get_law", {"law": LAW}, 3)
    log("get_law", ok and bool(law), list((law or {}).keys()) if ok else law)

    ok2, art = call_tool("get_article", {"law": LAW, "article": ARTICLE}, 4)
    text = ""
    if ok2 and isinstance(art, dict):
        text = art.get("text") or ""
        if not text:
            a = art.get("article")
            if isinstance(a, dict):
                text = a.get("text") or ""
            elif isinstance(a, str) and len(a) > 10:
                text = a
    log("get_article", ok2 and bool(text), (text or "")[:80] if ok2 else art)

    ok3, sa = call_tool("search_articles", {"keywords": "违约责任", "limit": 5}, 5)
    log("search_articles", ok3, str(sa)[:150] if ok3 else sa)

    ok4, fs = call_tool("flk_search", {"query": LAW}, 6)
    log("flk_search", ok4, str(fs)[:150] if ok4 else fs)

    ok5, lk = call_tool("lookup", {"query": "民法典第577条"}, 7)
    mc = (lk or {}).get("machine_conclusion", {}) if ok5 else {}
    log("lookup", ok5 and mc.get("law") == LAW, mc if ok5 else lk)

    ok6, vc = call_tool("verify_citation", {"law": LAW, "article": ARTICLE,
                                            "citation_text": text} if text else
              {"law": LAW, "article": ARTICLE, "citation_text": "x"}, 8)
    log("verify_citation", ok6 and (vc or {}).get("verdict") in ("verified", "exists_only"),
        (vc or {}).get("verdict") if ok6 else vc)

    ok7, cs = call_tool("check_support",
                        {"claim": "民法典第五百七十七条规定了违约责任。",
                         "citation_text": text} if text else
                        {"claim": "民法典第五百七十七条规定了违约责任。",
                         "citation_text": "x"}, 9)
    log("check_support", ok7 and (cs or {}).get("verdict") in
        ("supports", "partial", "unsupported", "cannot_determine"),
        (cs or {}).get("verdict") if ok7 else cs)

    ok8, cc = call_tool("check_citations_in_text",
                        {"text": "根据《中华人民共和国民法典》第五百七十七条，"
                                 "当事人一方不履行合同义务应当承担违约责任。"}, 10)
    log("check_citations_in_text", ok8 and (cc or {}).get("total_found", 0) >= 1,
        str(cc)[:200] if ok8 else cc)

    n_ok = sum(1 for r in recs if r["ok"])
    print(f"WORKER SMOKE {n_ok}/{len(recs)}", flush=True)
    return 0 if n_ok == len(recs) else 1


if __name__ == "__main__":
    sys.exit(main())
