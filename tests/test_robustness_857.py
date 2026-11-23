#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 第 857 项回归测试：实测缺陷四项。

- TestBadParamTypes：7+ 种错类型传法经 stdio 真实调用——只许返回
  工具错误，进程绝不退出，之后仍能正常服务；
- TestBadJsonLines：坏 JSON 行回 -32700、非对象回 -32600（与 HTTP
  传输对齐），进程存活；
- TestVerifySnapshotFallback：实时通道故障（函数级模拟）时
  verify_citation 自动降级快照通道，离线可核快照内引注且标注通道；
- TestInTextQuoteCompare：整段核验对带引号引文的条目做逐字比对，
  条号对、文字错必须被查出。

运行：python3 -m unittest tests.test_robustness_857 -v
（本文件全部离线：快照通道用仓库自带 data/snapshot.db。）
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

ART_577 = ("当事人一方不履行合同义务或者履行合同义务不符合约定的，"
           "应当承担继续履行、采取补救措施或者赔偿损失等违约责任。")
ART_578 = ("当事人一方明确表示或者以自己的行为表明不履行合同义务的，"
           "对方可以在履行期限届满前请求其承担违约责任。")


class StdioClient:
    def __init__(self):
        self.proc = subprocess.Popen(
            [sys.executable, SERVER], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, text=True, encoding="utf-8")
        self._id = 0
        self.rpc("initialize", {"protocolVersion": "2025-03-26",
                                "capabilities": {},
                                "clientInfo": {"name": "tests857",
                                               "version": "0"}})

    def send_raw(self, line):
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()
        return json.loads(self.proc.stdout.readline())

    def rpc(self, method, params=None):
        self._id += 1
        msg = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            msg["params"] = params
        return self.send_raw(json.dumps(msg))

    def call_raw(self, name, args):
        return self.rpc("tools/call", {"name": name, "arguments": args})

    def alive(self):
        return self.proc.poll() is None

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=10)
        except Exception:
            self.proc.kill()


class TestBadParamTypes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = StdioClient()

    @classmethod
    def tearDownClass(cls):
        cls.mcp.close()

    BAD_CALLS = [
        ("verify_citation", {"law": 123, "article": "第一条"}),
        ("verify_citation", {"law": "中华人民共和国民法典",
                             "article": ["第五百七十七条"]}),
        ("verify_citation", {"law": "中华人民共和国民法典",
                             "article": "第五百七十七条",
                             "citation_text": 577}),
        ("check_citations_in_text", {"text": 123}),
        ("lookup", {"query": {"law": "民法典"}}),
        ("flk_search", {"query": 123}),
        ("get_article", {"law": "中华人民共和国民法典",
                         "article": {"num": 577}}),
        ("check_support", {"claim": 123, "citation_text": ART_577}),
        ("search_articles", {"keywords": 123}),
        ("get_law", {"law": 123}),
    ]

    def test_bad_types_return_errors_and_process_survives(self):
        for name, args in self.BAD_CALLS:
            resp = self.mcp.call_raw(name, args)
            result = resp["result"]
            self.assertTrue(result.get("isError"),
                            f"{name} {args} 应返回工具错误：{resp}")
            text = result["content"][0]["text"]
            self.assertIn("类型不对", text,
                          f"{name} 错误信息应指出类型问题：{text}")
            self.assertTrue(self.mcp.alive(),
                            f"{name} 错类型传参后进程必须存活")
        # 进程仍能正常服务：离线工具调用成功
        resp = self.mcp.call_raw("check_support", {
            "claim": "当事人一方不履行合同义务的，应当承担继续履行、"
                     "采取补救措施或者赔偿损失等违约责任",
            "citation_text": ART_577})
        self.assertFalse(resp["result"].get("isError"))
        out = json.loads(resp["result"]["content"][0]["text"])
        self.assertEqual(out["verdict"], "supports")
        self.assertTrue(self.mcp.alive())

    def test_arguments_not_a_dict(self):
        resp = self.mcp.rpc("tools/call", {
            "name": "lookup", "arguments": ["民法典577条"]})
        self.assertTrue(resp["result"].get("isError"))
        self.assertIn("arguments 类型不对",
                      resp["result"]["content"][0]["text"])
        self.assertTrue(self.mcp.alive())


class TestBadJsonLines(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mcp = StdioClient()

    @classmethod
    def tearDownClass(cls):
        cls.mcp.close()

    def test_bad_json_line_gets_parse_error(self):
        resp = self.mcp.send_raw("{this is not json")
        self.assertIsNone(resp["id"])
        self.assertEqual(resp["error"]["code"], -32700)
        self.assertTrue(self.mcp.alive())

    def test_non_object_json_gets_invalid_request(self):
        resp = self.mcp.send_raw("[1, 2, 3]")
        self.assertEqual(resp["error"]["code"], -32600)
        self.assertTrue(self.mcp.alive())
        # 之后仍可正常 initialize/调用
        resp2 = self.mcp.rpc("tools/list")
        self.assertEqual(len(resp2["result"]["tools"]), 8)


class TestVerifySnapshotFallback(unittest.TestCase):
    """实时通道整体故障时（模拟断网），verify 走快照通道。"""

    def setUp(self):
        self._orig_find = srv.find_law

        def _down(name):
            raise srv.SourceError(
                "向官方源发请求这一步没走通：无法连接国家法律法规"
                "数据库（网络不通或连接被中途中断）。下一步：①稍后重试")
        srv.find_law = _down

    def tearDown(self):
        srv.find_law = self._orig_find

    def test_snapshot_covered_citation_verifies_offline(self):
        out = srv.tool_verify_citation({
            "law": "中华人民共和国民法典",
            "article": "第五百七十七条",
            "citation_text": ART_577})
        self.assertEqual(out["verdict"], "verified")
        self.assertEqual(out["data_snapshot"]["channel"], "snapshot")
        self.assertTrue(out.get("degraded"))
        self.assertIn("快照", out["notes"][0])
        self.assertTrue(out["checks"]["text"]["exact_match"])

    def test_snapshot_text_mismatch_still_caught_offline(self):
        out = srv.tool_verify_citation({
            "law": "中华人民共和国民法典",
            "article": "第五百七十七条",
            "citation_text": ART_578})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertFalse(out["checks"]["text"]["exact_match"])

    def test_uncovered_law_raises_human_error(self):
        with self.assertRaises(srv.SourceError) as ctx:
            srv.tool_verify_citation({
                "law": "中华人民共和国某部未收录的法",
                "article": "第一条"})
        msg = str(ctx.exception)
        self.assertNotIn("URLError", msg)
        self.assertNotIn("RemoteDisconnected", msg)
        self.assertIn("下一步", msg)


class TestInTextQuoteCompare(unittest.TestCase):
    """整段核验：带引号引文的条目必须做逐字比对（实时故障下走快照）。"""

    def setUp(self):
        self._orig_find = srv.find_law

        def _down(name):
            raise srv.SourceError(
                "向官方源发请求这一步没走通：无法连接国家法律法规"
                "数据库（网络不通或连接被中途中断）。下一步：①稍后重试")
        srv.find_law = _down

    def tearDown(self):
        srv.find_law = self._orig_find

    def test_correct_quote_verified_with_text_check(self):
        text = ("依据《中华人民共和国民法典》第五百七十七条规定："
                f"「{ART_577}」处理。")
        out = srv.tool_check_citations_in_text({"text": text})
        self.assertEqual(out["total_found"], 1)
        entry = out["results"][0]
        self.assertTrue(entry["quoted_text_found"])
        self.assertEqual(entry["verdict"], "verified")
        self.assertTrue(entry["text_check"]["exact_match"])

    def test_wrong_quote_caught(self):
        text = ("依据《中华人民共和国民法典》第五百七十七条规定："
                f"「{ART_578}」处理。")
        out = srv.tool_check_citations_in_text({"text": text})
        entry = out["results"][0]
        self.assertTrue(entry["quoted_text_found"])
        self.assertEqual(entry["verdict"], "not_verified")
        self.assertFalse(entry["text_check"]["exact_match"])
        self.assertTrue(any("text" in p for p in entry["problems"]))

    def test_no_quote_means_no_text_check_and_says_so(self):
        text = "依据《中华人民共和国民法典》第五百七十七条处理。"
        out = srv.tool_check_citations_in_text({"text": text})
        entry = out["results"][0]
        self.assertFalse(entry["quoted_text_found"])
        self.assertIsNone(entry["text_check"]["exact_match"])
        self.assertIn("未做逐字比对", entry["text_check"]["detail"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
