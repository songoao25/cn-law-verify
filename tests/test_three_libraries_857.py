#!/usr/bin/env python3
"""857 三库分工测试：moj（司法部行政法规库）＋ gzk（国家规章库）。

离线部分：HTML/JSON 解析用合成夹具；集成兜底用 monkeypatch 隔离网络。
联网部分：CN_LAW_VERIFY_LIVE=0 时跳过。
"""
import json
import os
import sys
import unittest
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "server"))
import cn_law_verify_server as srv  # noqa: E402

LIVE = os.environ.get("CN_LAW_VERIFY_LIVE", "1") != "0"

MOJ_LIST_HTML = """
<html><body>
<input type="hidden" class="law-total" value="42">
<div class="incident-record">
  <a href="/front/law/detail?LawID=101">中华人民共和国测试行政法规</a>
  <span data-time="2000-01-01"></span>
</div>
<div class="incident-record">
  <a href="/front/law/detail?LawID=102">另一部测试法规</a>
</div>
</body></html>
"""

MOJ_DETAIL_HTML = """
<html><body>
<div class="text-title">中华人民共和国测试行政法规</div>
<div class="info">时效性：现行有效</div>
<div class="law-chapter"><div>
<p>（2000年1月1日国务院令第1号公布 自2000年2月1日起施行）</p>
<p>第一条 为了测试，制定本法规。</p>
<p>第二条 测试对象应当遵守本法规。</p>
</div></div>
<div class="fold-con"><div class="fold-con-inner">
<a href="/front/law/detail?LawID=99">中华人民共和国测试行政法规（旧版）</a>
</div></div>
</body></html>
"""

FAKE_MOJ_DETAIL = {
    "id": "12345", "title": "中华人民共和国测试行政法规",
    "paragraphs": ["第一条 为了测试，制定本法规。",
                   "第二条 测试对象应当遵守本法规。"],
    "status_zh": "现行有效", "status_code": 3,
    "promulgated": "2000年1月1日", "effective_from": "2000年2月1日",
    "authority": "国务院", "history": [],
    "source_url": "https://xzfg.moj.gov.cn/front/law/detail?LawID=12345",
}


class TestMojParsing(unittest.TestCase):
    def test_parse_list_incident_record(self):
        items = srv._moj_parse_list(MOJ_LIST_HTML)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["id"], "101")
        self.assertEqual(items[0]["title"], "中华人民共和国测试行政法规")
        self.assertEqual(items[0]["library"], "moj")

    def test_status_from_html_explicit(self):
        self.assertEqual(
            srv._moj_status_from_html("<p>时效性：已废止</p>"), "已废止")

    def test_status_from_html_unknown_is_empty(self):
        # 解析不到效力时如实未核，绝不兜底冒充现行有效
        self.assertEqual(srv._moj_status_from_html("<p>无关页面</p>"), "")

    def test_fetch_detail_with_fixture(self):
        orig = srv._fetch_alt_text
        srv._fetch_alt_text = lambda url, label, site: MOJ_DETAIL_HTML
        try:
            d = srv.moj_fetch_detail("101")
        finally:
            srv._fetch_alt_text = orig
        self.assertEqual(d["title"], "中华人民共和国测试行政法规")
        self.assertEqual(d["status_code"], 3)
        self.assertEqual(d["promulgated"], "2000年1月1日")
        self.assertEqual(d["effective_from"], "2000年2月1日")
        self.assertEqual(d["authority"], "国务院")
        self.assertEqual(len(d["paragraphs"]), 3)
        self.assertEqual(len(d["history"]), 1)
        self.assertEqual(d["history"][0]["id"], "99")


class TestGzkCryptoAndParsing(unittest.TestCase):
    def test_rsa_params_parse(self):
        n, e = srv._gzk_rsa_params()
        self.assertEqual(e, 65537)
        self.assertEqual((n.bit_length() + 7) // 8, 128)

    def test_app_key_header_shape(self):
        header = srv._gzk_app_key_header()
        raw = urllib.parse.unquote(header)
        import base64
        self.assertEqual(len(base64.b64decode(raw)), 128)

    def test_search_rows_with_fixture(self):
        payload = {
            "result": {"data": {
                "middle": {"list": [{
                    "title_no_tag": "测试部门管理办法",
                    "title": "<em>测试部门管理办法</em>",
                    "url": "https://example.gov.cn/x.html",
                    "time": "2021-05-01",
                    "source": "测试部网站",
                    "agencies": ["测试部"],
                    "documentId": "doc-1",
                    "content": "摘要内容",
                }]},
                "pager": {"total": 7},
            }},
        }
        orig = srv._request
        srv._request = lambda *a, **k: json.dumps(payload).encode("utf-8")
        try:
            total, items = srv.gzk_search_rows("测试部门管理办法")
        finally:
            srv._request = orig
        self.assertEqual(total, 7)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["title"], "测试部门管理办法")
        self.assertIsNone(items[0]["status_code"])
        self.assertTrue(items[0]["inferred"])
        self.assertEqual(items[0]["library"], "gzk")
        self.assertIn("效力未核", items[0]["status"])


class TestFallbackIntegration(unittest.TestCase):
    """verify/get_article 在 flk 未收录时走 moj/gzk 兜底（网络全隔离）。"""

    def setUp(self):
        self._orig = {name: getattr(srv, name) for name in
                      ("find_law", "moj_find_law", "gzk_search_rows")}
        srv.find_law = lambda name: (None, None)

    def tearDown(self):
        for name, fn in self._orig.items():
            setattr(srv, name, fn)

    def test_verify_via_moj_verified(self):
        srv.moj_find_law = lambda name: dict(FAKE_MOJ_DETAIL)
        out = srv.tool_verify_citation({
            "law": "中华人民共和国测试行政法规", "article": "第二条",
            "citation_text": "测试对象应当遵守本法规。"})
        self.assertEqual(out["verdict"], "verified")
        self.assertEqual(out["law"]["library"], "moj")
        self.assertEqual(out["checks"]["text"]["exact_match"], True)

    def test_verify_via_moj_text_mismatch(self):
        srv.moj_find_law = lambda name: dict(FAKE_MOJ_DETAIL)
        out = srv.tool_verify_citation({
            "law": "中华人民共和国测试行政法规", "article": "第二条",
            "citation_text": "测试对象可以不遵守本法规。"})
        self.assertEqual(out["verdict"], "not_verified")

    def test_verify_via_moj_repealed_blocked(self):
        detail = dict(FAKE_MOJ_DETAIL, status_code=1, status_zh="已废止")
        srv.moj_find_law = lambda name: detail
        out = srv.tool_verify_citation({
            "law": "中华人民共和国测试行政法规", "article": "第二条",
            "citation_text": "测试对象应当遵守本法规。"})
        self.assertEqual(out["verdict"], "not_verified")

    def test_verify_via_gzk_exists_only(self):
        srv.moj_find_law = lambda name: None
        srv.gzk_search_rows = lambda q, mode="title", limit=10: (1, [{
            "id": "d1", "title": "某市测试管理办法", "authority": "某市人民政府",
            "category": "地方政府规章", "inferred": True,
            "promulgated": "2022-01-01", "effective_from": "",
            "status": "效力未核（国家规章库无官方效力字段）",
            "status_en": "unverified", "status_code": None,
            "source_url": "https://example.gov.cn/r.html", "summary": "",
            "library": "gzk"}])
        out = srv.tool_verify_citation({
            "law": "某市测试管理办法", "article": "第一条",
            "citation_text": "任意引文"})
        self.assertEqual(out["verdict"], "exists_only")
        self.assertFalse(out["checks"]["status"]["ok"])
        self.assertIn("效力未核", out["checks"]["status"]["detail"])

    def test_verify_all_libraries_miss(self):
        srv.moj_find_law = lambda name: None
        srv.gzk_search_rows = lambda q, mode="title", limit=10: (0, [])
        out = srv.tool_verify_citation({
            "law": "中华人民共和国不存在法", "article": "第一条"})
        self.assertEqual(out["verdict"], "not_verified")
        self.assertIn("三个官方库", out["notes"][0])

    def test_get_article_via_moj(self):
        srv.moj_find_law = lambda name: dict(FAKE_MOJ_DETAIL)
        out = srv.tool_get_article({
            "law": "中华人民共和国测试行政法规", "article": "第二条"})
        self.assertEqual(out["text"], "测试对象应当遵守本法规。")
        self.assertEqual(out["text_source"], "moj_official_text")
        self.assertEqual(out["law"]["library"], "moj")


@unittest.skipUnless(LIVE, "联网集成用例（设 CN_LAW_VERIFY_LIVE=0 跳过）")
class TestThreeLibrariesLive(unittest.TestCase):
    def test_moj_search_live(self):
        # moj 服务端检索过滤当前未生效（2026-11-23 实测）：只断言通道
        # 可达且本地过滤后的条目都含检索词，不断言必有命中。
        total, items = srv.moj_search_rows("管理办法", limit=5)
        self.assertTrue(all("管理办法" in i["title"] for i in items))

    def test_moj_detail_live(self):
        # 详情通道可用（LawID=7 为库内最早期行政法规之一）
        d = srv.moj_fetch_detail("7")
        self.assertIn("政务院", d["title"])
        self.assertTrue(d["paragraphs"])

    def test_gzk_search_live(self):
        total, items = srv.gzk_search_rows("管理办法", limit=5)
        self.assertGreaterEqual(len(items), 1)
        self.assertTrue(all(i["status_code"] is None for i in items))

    def test_flk_search_auto_live(self):
        out = srv.tool_flk_search({"query": "民法典", "limit": 3})
        self.assertIn("totals_by_library", out)
        self.assertTrue(out["results"])


if __name__ == "__main__":
    unittest.main()
