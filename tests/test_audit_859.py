#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""859 对抗审计回归测试（离线，纯函数与协议层）。

覆盖 859 打穿并修复的点：全角/繁体条号、无书名号引注抽取、
cn_to_int 静默错位、引文抽取上限、check_support v3 新信号
（方向/序号污染/序数称谓/主体宾语误绑/排他对齐）、stdio 批量
与 HTTP 批量口径对齐、moj 本地标题索引加载。
联网部分不在此文件（见 test_full_tools.py 等）。
"""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "server"))
os.environ.setdefault("CN_LAW_VERIFY_MOJ_INDEX",
                      str(ROOT / "data" / "moj_index.json"))
import cn_law_verify_server as srv  # noqa: E402


class TestNormalize859(unittest.TestCase):
    def test_fullwidth_digits(self):
        self.assertEqual(srv.normalize_article("第５７７条"), (577, None))

    def test_traditional_tiao(self):
        self.assertEqual(srv.normalize_article("第五百七十七條"), (577, None))

    def test_digit_run_without_unit_rejected(self):
        # 「五七七」无单位连写：857 版静默解析成 7（核验到完全不同的
        # 条文还不自知），859 起往返校验判非法
        with self.assertRaises(ValueError):
            srv.normalize_article("第五七七条")

    def test_trailing_sub_clause_tolerated(self):
        # 尾随款/项定位：核验到条一级，忽略不报错（859 定版）
        self.assertEqual(srv.normalize_article("第577条第2款"), (577, None))

    def test_float_rejected(self):
        with self.assertRaises(ValueError):
            srv.normalize_article("577.5")

    def test_cn_variants(self):
        self.assertEqual(srv.cn_to_int("两百"), 200)
        self.assertEqual(srv.cn_to_int("一十"), 10)
        with self.assertRaises(ValueError):
            srv.cn_to_int("五七七")


class TestExtract859(unittest.TestCase):
    def test_bare_citation_short_name(self):
        out = srv.extract_citations("根据民法典第五百七十七条处理。")
        self.assertEqual([(c["law"], c["article_num"]) for c in out],
                         [("民法典", 577)])

    def test_bare_citation_full_name(self):
        out = srv.extract_citations(
            "根据中华人民共和国民法典第五百七十七条处理。")
        self.assertEqual(out[0]["law"], "中华人民共和国民法典")

    def test_bare_self_ref_not_citation(self):
        self.assertEqual(srv.extract_citations("本法第五百七十七条另有规定。"),
                         [])

    def test_traditional_law_name(self):
        out = srv.extract_citations("依《中華人民共和國民法典》第五百七十七條。")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["article_num"], 577)

    def test_quote_runaway_capped(self):
        text = "《民法典》第五百七十七条规定：“甲段。" + "无关文字。" * 900 + "”"
        cit = srv.extract_citations(text)[0]
        q = srv._extract_following_quote(text, cit["span"][1])
        self.assertTrue(q is None or len(q) <= srv._MAX_QUOTED_LEN)


class TestSupportV3(unittest.TestCase):
    def verdict(self, claim, cite):
        return srv.tool_check_support(
            {"claim": claim, "citation_text": cite})["verdict"]

    def test_direction_swap_single_tier(self):
        self.assertEqual(
            self.verdict("处三年以上有期徒刑。",
                         "处三年以下有期徒刑、拘役或者管制。"),
            "unsupported")

    def test_enumerator_not_a_number(self):
        # 引文的款项序号（三）不许给论断编造的「三倍」当依据
        self.assertEqual(srv._extract_numbers("（三）未及时足额支付劳动报酬的"),
                         [])
        self.assertIn((3.0, "倍"), srv._extract_numbers("支付三倍工资"))

    def test_ordinal_party_not_a_number(self):
        self.assertEqual(srv._extract_numbers("由第三人代为履行"), [])

    def test_quantities_directions(self):
        self.assertEqual(srv._extract_quantities("三年以上"),
                         {(3.0, "年"): {"above"}})
        self.assertEqual(srv._extract_quantities("不满一年"),
                         {(1.0, "年"): {"strict"}})
        self.assertEqual(srv._extract_quantities("不超过三年"),
                         {(3.0, "年"): {"below"}})

    def test_object_not_actor(self):
        # 「对劳动者造成损害」的劳动者是宾语，忠实论断不许误报主体偷换
        self.assertEqual(
            self.verdict(
                "因用人单位的原因订立的无效劳动合同，对劳动者造成损害的，"
                "应当由用人单位承担赔偿责任。",
                "由于用人单位的原因订立的无效劳动合同，对劳动者造成损害的，"
                "应当承担赔偿责任。"),
            "supports")

    def test_no_cap_conflict(self):
        self.assertEqual(
            self.verdict(
                "经济补偿的月工资标准没有上限，挣多少按多少算。",
                "向其支付经济补偿的标准按职工月平均工资三倍的数额支付，"
                "向其支付经济补偿的年限最高不超过十二年。"),
            "unsupported")

    def test_entity_form_swap(self):
        self.assertEqual(
            self.verdict(
                "股份有限公司的注册资本为在公司登记机关登记的已发行股本总额。",
                "有限责任公司的注册资本为在公司登记机关登记的全体股东"
                "认缴的出资额。"),
            "unsupported")

    def test_exclusive_alignment(self):
        # 引文「非…不可」也是排他：论断用「只能」转述不算偷换
        self.assertEqual(
            self.verdict(
                "订立劳动合同，只能采用书面形式。",
                "建立劳动关系，非订立书面劳动合同不可；已建立劳动关系，"
                "未同时订立书面劳动合同的，应当自用工之日起一个月内"
                "订立书面劳动合同。"),
            "supports")


class TestMojIndex(unittest.TestCase):
    def test_index_loaded_and_covers_beyond_first_page(self):
        idx = srv._moj_index()
        self.assertGreater(len(idx["items"]), 1000)
        total, rows = srv.moj_search_rows("物业管理条例", mode="title")
        titles = [r["title"] for r in rows]
        self.assertIn("物业管理条例", titles)
        self.assertGreaterEqual(total, 1)


class TestTransports859(unittest.TestCase):
    def test_stdio_batch(self):
        proc = subprocess.run(
            [sys.executable, str(ROOT / "server" / "cn_law_verify_server.py")],
            input='[{"jsonrpc":"2.0","id":1,"method":"ping"},42,'
                  '{"jsonrpc":"2.0","id":2,"method":"ping"}]\n',
            capture_output=True, text=True, timeout=30)
        lines = [l for l in proc.stdout.splitlines() if l.strip()]
        self.assertEqual(len(lines), 1)
        resp = json.loads(lines[0])
        self.assertIsInstance(resp, list)
        codes = [r.get("error", {}).get("code") for r in resp]
        self.assertIn(-32600, codes)
        ids = [r.get("id") for r in resp]
        self.assertIn(1, ids)
        self.assertIn(2, ids)


if __name__ == "__main__":
    unittest.main()
