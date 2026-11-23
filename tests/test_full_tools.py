#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 全量工具测试（第 842 项）。

- TestOfflineTools：lookup 解析、check_support 启发式四档、split_articles、
  快照读写（合成小库）、HTTP 传输层（本地起服务，只调离线可完成的工具）。
- TestLiveTools：经 stdio 真实调用 server、真实查询官方源：
  get_law / get_article（快照与实时）/ search_articles /
  check_citations_in_text / lookup / check_support（law+article 模式）。

运行：python3 -m unittest discover -s tests -v
联网用例可用 CN_LAW_VERIFY_LIVE=0 跳过。
"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server", "cn_law_verify_server.py")
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402

ART_577 = ("当事人一方不履行合同义务或者履行合同义务不符合约定的，"
           "应当承担继续履行、采取补救措施或者赔偿损失等违约责任。")


class TestOfflineTools(unittest.TestCase):
    def test_lookup_parse_forms(self):
        cases = [
            ("民法典577条", "民法典", 577, None),
            ("《中华人民共和国劳动合同法》第三十九条",
             "中华人民共和国劳动合同法", 39, None),
            ("劳动合同法第三十九条是什么", "劳动合同法", 39, None),
            ("民诉法第64条", "民诉法", 64, None),
            ("民法典第五百七十七条之一", "民法典", 577, 1),
            ("查一下公司法", "公司法", None, None),
        ]
        for query, law, num, sub in cases:
            got = srv.parse_lookup_query(query)
            self.assertEqual((got[0], got[1], got[2]), (law, num, sub),
                             f"解析 {query} 失败：{got}")

    def test_support_supports(self):
        out = srv.tool_check_support({
            "claim": "当事人一方不履行合同义务的，应当承担继续履行、"
                     "采取补救措施或者赔偿损失等违约责任",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "supports")
        self.assertIn("不能替代人工判断", out["disclaimer"])

    def test_support_number_mismatch(self):
        out = srv.tool_check_support({
            "claim": "违约方应当在十日内支付赔偿金一百万元",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "unsupported")
        self.assertTrue(any("数字" in r for r in out["reasons"]))

    def test_support_polarity_conflict(self):
        out = srv.tool_check_support({
            "claim": "当事人一方不履行合同义务时，可以不承担任何违约责任",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "unsupported")
        self.assertTrue(any("极性冲突" in r for r in out["reasons"]))

    def test_support_partial(self):
        out = srv.tool_check_support({
            "claim": "当事人不履行合同义务应当承担赔偿损失等违约责任，"
                     "此外合同成立后当事人应当遵循诚信原则并及时通知对方有关情况",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "partial")

    def test_support_cannot_determine(self):
        out = srv.tool_check_support({
            "claim": "公司应当依法设立董事会并定期召开股东会",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "cannot_determine")

    def test_split_articles_dedup_prefers_longer(self):
        paras = ["第一条", "第一条 正文内容更长的一份。",
                 "第二条 第二条内容。"]
        arts = srv.split_articles(paras)
        self.assertEqual(len(arts), 2)
        self.assertEqual(arts[0]["text"], "正文内容更长的一份。")

    def _make_snapshot(self, path):
        conn = sqlite3.connect(path)
        conn.executescript("""
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE laws(bbbs TEXT PRIMARY KEY, title TEXT, authority TEXT,
            category TEXT, promulgated TEXT, effective_from TEXT,
            status_code INTEGER, source_url TEXT, toc_json TEXT,
            article_count INTEGER, fetched_at TEXT);
        CREATE TABLE articles(bbbs TEXT, num INTEGER, sub INTEGER,
            heading TEXT, text TEXT, PRIMARY KEY(bbbs, num, sub));
        """)
        conn.execute("INSERT INTO meta VALUES ('built_at', '2026-01-01T00:00:00+00:00')")
        conn.execute("INSERT INTO meta VALUES ('coverage', '[]')")
        conn.execute(
            "INSERT INTO laws VALUES ('TESTID','中华人民共和国测试法','测试机关',"
            "'法律','2020-01-01','2020-06-01',3,"
            "'https://flk.npc.gov.cn/detail.html?bbbs=TESTID','[]',2,"
            "'2026-01-01T00:00:00+00:00')")
        conn.executemany(
            "INSERT INTO articles VALUES (?,?,?,?,?)",
            [("TESTID", 1, 0, "第一条", "为了测试快照通道，制定本法。"),
             ("TESTID", 2, 0, "第二条", "竞业限制人员不得自营同类业务。")])
        conn.commit()
        conn.close()

    def test_snapshot_read_path(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "snapshot.db")
            self._make_snapshot(db)
            old = os.environ.get("CN_LAW_VERIFY_SNAPSHOT_DB")
            os.environ["CN_LAW_VERIFY_SNAPSHOT_DB"] = db
            try:
                info = srv.snapshot_info()
                self.assertEqual(info["law_count"], 1)
                self.assertEqual(info["article_count"], 2)
                law = srv.snapshot_find_law("测试法")
                self.assertEqual(law["title"], "中华人民共和国测试法")
                art = srv.snapshot_get_article("TESTID", 2)
                self.assertIn("竞业限制", art["text"])
                hits = srv.snapshot_search(["竞业限制"], limit=5)
                self.assertEqual(len(hits), 1)
                self.assertEqual(hits[0]["article"], "第二条")
                out = srv.tool_get_article(
                    {"law": "中华人民共和国测试法", "article": "第二条"})
                self.assertEqual(out["text_source"], "local_snapshot")
                self.assertEqual(out["data_snapshot"]["channel"], "snapshot")
                sout = srv.tool_search_articles({"keywords": "竞业限制"})
                self.assertEqual(sout["data_snapshot"]["channel"], "snapshot")
                self.assertIn("快照", sout["coverage"])
            finally:
                if old is None:
                    os.environ.pop("CN_LAW_VERIFY_SNAPSHOT_DB", None)
                else:
                    os.environ["CN_LAW_VERIFY_SNAPSHOT_DB"] = old


class TestHttpTransport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import socket
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        cls.port = sock.getsockname()[1]
        sock.close()
        cls.thread = threading.Thread(
            target=srv.serve_http, args=("127.0.0.1", cls.port), daemon=True)
        cls.thread.start()
        import time
        time.sleep(0.5)

    def _post(self, payload):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/mcp",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def test_http_tools_list(self):
        status, body = self._post({"jsonrpc": "2.0", "id": 1,
                                   "method": "tools/list"})
        self.assertEqual(status, 200)
        names = {t["name"] for t in body["result"]["tools"]}
        self.assertEqual(len(names), 8)

    def test_http_offline_tool_call(self):
        status, body = self._post({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "check_support",
                       "arguments": {"claim": "公司应当依法设立董事会",
                                     "citation_text": ART_577}}})
        self.assertEqual(status, 200)
        result = body["result"]
        self.assertFalse(result["isError"])
        out = json.loads(result["content"][0]["text"])
        self.assertEqual(out["verdict"], "cannot_determine")


class McpClient:
    def __init__(self):
        self.proc = subprocess.Popen(
            [sys.executable, SERVER], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, text=True, encoding="utf-8")
        self._id = 0
        self.rpc("initialize", {"protocolVersion": "2025-03-26",
                                "capabilities": {},
                                "clientInfo": {"name": "tests", "version": "0"}})
        self.proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        self.proc.stdin.flush()

    def rpc(self, method, params=None):
        self._id += 1
        msg = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            msg["params"] = params
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()
        return json.loads(self.proc.stdout.readline())

    def call(self, name, args):
        resp = self.rpc("tools/call", {"name": name, "arguments": args})
        result = resp["result"]
        text = result["content"][0]["text"]
        if result.get("isError"):
            raise AssertionError(f"工具 {name} 返回错误：{text}")
        return json.loads(text)

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=10)


@unittest.skipUnless(os.environ.get("CN_LAW_VERIFY_LIVE") != "0",
                     "联网集成用例（设 CN_LAW_VERIFY_LIVE=0 跳过）")
class TestLiveTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = McpClient()

    @classmethod
    def tearDownClass(cls):
        cls.mcp.close()

    def test_get_law_civil_code(self):
        out = self.mcp.call("get_law", {"law": "中华人民共和国民法典"})
        self.assertEqual(out["law"]["title"], "中华人民共和国民法典")
        self.assertEqual(out["law"]["status"], "有效")
        self.assertEqual(out["article_count"], 1260)
        self.assertTrue(out["outline"])

    def test_get_law_company_versions(self):
        out = self.mcp.call("get_law", {"law": "中华人民共和国公司法"})
        self.assertGreaterEqual(len(out["versions"]), 3)
        years = [v["promulgated"][:4] for v in out["versions"]]
        self.assertIn("2023", years)
        self.assertIn("2018", years)

    def test_get_article_live_or_snapshot(self):
        out = self.mcp.call("get_article", {
            "law": "中华人民共和国民法典", "article": "第五百七十七条"})
        self.assertEqual(out["text"], ART_577)
        self.assertIn(out["text_source"],
                      ("local_snapshot", "official_docx_full_text"))

    def test_get_article_missing(self):
        resp = self.mcp.rpc("tools/call", {
            "name": "get_article",
            "arguments": {"law": "中华人民共和国国歌法",
                          "article": "第五十条"}})
        self.assertTrue(resp["result"]["isError"])

    def test_search_articles_snapshot(self):
        info = srv.snapshot_info()
        if info is None:
            self.skipTest("快照库尚未构建")
        out = self.mcp.call("search_articles", {"keywords": "违约责任"})
        self.assertTrue(out["results"])
        self.assertTrue(any(r["law_title"] == "中华人民共和国民法典"
                            for r in out["results"]))

    def test_search_articles_live_fallback(self):
        old = os.environ.get("CN_LAW_VERIFY_SNAPSHOT_DB")
        os.environ["CN_LAW_VERIFY_SNAPSHOT_DB"] = "/nonexistent/snapshot.db"
        try:
            out = srv.tool_search_articles(
                {"keywords": "饲养的动物造成他人损害"})
        finally:
            if old is None:
                os.environ.pop("CN_LAW_VERIFY_SNAPSHOT_DB", None)
            else:
                os.environ["CN_LAW_VERIFY_SNAPSHOT_DB"] = old
        self.assertEqual(out["data_snapshot"]["channel"], "live")
        self.assertIn("不是全库检索", out["coverage"])
        self.assertTrue(out["results"])

    def test_check_citations_in_text(self):
        text = ("依据《中华人民共和国民法典》第五百七十七条处理；"
                "另见《中华人民共和国太空旅游法》第三条与"
                "《中华人民共和国国歌法》第五十条。")
        out = self.mcp.call("check_citations_in_text", {"text": text})
        self.assertEqual(out["total_found"], 3)
        by_law = {r["law"]: r for r in out["results"]}
        self.assertEqual(by_law["中华人民共和国民法典"]["verdict"], "verified")
        self.assertEqual(by_law["中华人民共和国太空旅游法"]["verdict"],
                         "not_verified")
        self.assertEqual(by_law["中华人民共和国国歌法"]["verdict"],
                         "not_verified")
        self.assertEqual(out["summary"].get("verified"), 1)
        self.assertEqual(len(out["problems"]), 2)

    def test_lookup_colloquial(self):
        out = self.mcp.call("lookup", {"query": "民法典577条"})
        mc = out["machine_conclusion"]
        self.assertEqual(mc["kind"], "article")
        self.assertEqual(mc["text"], ART_577)
        self.assertTrue(out["human_review_checklist"])

    def test_lookup_law_only(self):
        out = self.mcp.call("lookup", {"query": "查一下劳动合同法"})
        mc = out["machine_conclusion"]
        self.assertEqual(mc["kind"], "law")
        self.assertEqual(mc["law"], "中华人民共和国劳动合同法")

    def test_check_support_live_mode(self):
        out = self.mcp.call("check_support", {
            "claim": "当事人一方不履行合同义务的，应当承担继续履行、"
                     "采取补救措施或者赔偿损失等违约责任",
            "law": "中华人民共和国民法典", "article": "第五百七十七条"})
        self.assertEqual(out["verdict"], "supports")
        self.assertEqual(out["citation"]["law"], "中华人民共和国民法典")


if __name__ == "__main__":
    unittest.main(verbosity=2)
