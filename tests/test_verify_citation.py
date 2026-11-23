#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 测试。

两组：
- TestOffline：纯函数单测（条号归一化、引文抽取、条文截取、域名白名单），
  不联网，随时可跑；
- TestVerifyCitationLive：经 stdio 真实调用 server、真实查询官方源的
  集成用例（需联网）。覆盖四类必测情形：真法条命中、编造法名、
  条号漂移、已废止旧条文。

运行：python3 -m unittest discover -s tests -v
     或：python3 tests/test_verify_citation.py
"""

import json
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server", "cn_law_verify_server.py")
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402


class TestOffline(unittest.TestCase):
    def test_cn_numerals_roundtrip(self):
        for n, cn in [(1, "一"), (10, "十"), (16, "十六"), (107, "一百零七"),
                      (577, "五百七十七"), (1260, "一千二百六十")]:
            self.assertEqual(srv.int_to_cn(n), cn)
            self.assertEqual(srv.cn_to_int(cn), n)

    def test_normalize_article_forms(self):
        self.assertEqual(srv.normalize_article(577), (577, None))
        self.assertEqual(srv.normalize_article("第577条"), (577, None))
        self.assertEqual(srv.normalize_article("第五百七十七条"), (577, None))
        self.assertEqual(srv.normalize_article("第五百七十七条之一"), (577, 1))

    def test_extract_article_from_paragraphs(self):
        paras = ["第一条 为了测试。",
                 "第二条 第一段。", "第二段继续。",
                 "第三条 另一条。"]
        art = srv.extract_article(paras, 2)
        self.assertEqual(art["text"], "第一段。\n第二段继续。")
        self.assertIsNone(srv.extract_article(paras, 9))

    def test_extract_citations(self):
        text = "依《中华人民共和国民法典》第五百七十七条与《中华人民共和国国歌法》第3条处理。"
        cites = srv.extract_citations(text)
        self.assertEqual(len(cites), 2)
        self.assertEqual(cites[0]["article"], "第五百七十七条")
        self.assertEqual(cites[1]["article_num"], 3)

    def test_url_whitelist(self):
        srv._check_url("https://flk.npc.gov.cn/law-search/search/list")
        with self.assertRaises(srv.SourceError):
            srv._check_url("http://flk.npc.gov.cn/x")
        with self.assertRaises(srv.SourceError):
            srv._check_url("https://example.com/x")
        with self.assertRaises(srv.SourceError):
            srv._check_url("https://flkoss.obs-bj2.cucloud.cn/x.docx")
        srv._check_url("https://flkoss.obs-bj2.cucloud.cn/x.docx",
                       for_download=True)


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
class TestVerifyCitationLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = McpClient()

    @classmethod
    def tearDownClass(cls):
        cls.mcp.close()

    def test_real_citation_verified(self):
        """真法条命中：民法典第577条＋官方原文 → 三检全过。"""
        out = self.mcp.call("verify_citation", {
            "law": "中华人民共和国民法典",
            "article": "第五百七十七条",
            "citation_text": "当事人一方不履行合同义务或者履行合同义务不符合约定的，"
                             "应当承担继续履行、采取补救措施或者赔偿损失等违约责任。"})
        self.assertEqual(out["verdict"], "verified")
        self.assertTrue(out["checks"]["existence"]["ok"])
        self.assertTrue(out["checks"]["status"]["ok"])
        self.assertTrue(out["checks"]["text"]["exact_match"])

    def test_fabricated_law_rejected(self):
        """编造法名：不存在的法规必须被存在性检查拦下。"""
        out = self.mcp.call("verify_citation", {
            "law": "中华人民共和国太空旅游法", "article": "第一条"})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertFalse(out["checks"]["existence"]["ok"])

    def test_article_number_drift(self):
        """条号漂移：国歌法只有 16 条，第 50 条不存在。"""
        out = self.mcp.call("verify_citation", {
            "law": "中华人民共和国国歌法", "article": "第五十条"})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertTrue(out["checks"]["existence"]["ok"])
        self.assertFalse(out["checks"]["article"]["ok"])

    def test_text_mismatch(self):
        """条号对、文本错位：577 条配 578 条的文本 → 逐字比对不通过。"""
        out = self.mcp.call("verify_citation", {
            "law": "中华人民共和国民法典",
            "article": "第五百七十七条",
            "citation_text": "当事人一方明确表示或者以自己的行为表明不履行合同义务的，"
                             "对方可以在履行期限届满前请求其承担违约责任。"})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertFalse(out["checks"]["text"]["exact_match"])

    def test_repealed_law_flagged(self):
        """已废止旧条文：合同法已随民法典施行废止，必须被效力检查拦下。"""
        out = self.mcp.call("verify_citation", {
            "law": "中华人民共和国合同法",
            "article": "第一百零七条",
            "citation_text": "当事人一方不履行合同义务或者履行合同义务不符合约定的，"
                             "应当承担继续履行、采取补救措施或者赔偿损失等违约责任。"})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertTrue(out["checks"]["existence"]["ok"])
        self.assertFalse(out["checks"]["status"]["ok"])
        self.assertEqual(out["checks"]["status"]["status"], "已废止")


if __name__ == "__main__":
    unittest.main(verbosity=2)
