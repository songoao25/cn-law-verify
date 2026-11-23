#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""lookup 法名近名纠错测试（第 843b 项）。

- TestNearNameOffline：_near_name_match 纯函数——开发集错别字 20 例
  的解析名逐个应唯一纠正到期望法规；阈值、并列交人挑、同音破
  并列用合成小全集确定性验证。
- TestVerifyIsolation：核验路径不得带纠错——结构上 find_law /
  snapshot_find_law 不引用纠错函数；联网时 verify_citation 对
  写错的法名必须判 not_verified，不许被纠正放行。
- TestLookupLive：联网集成——lookup 对错别字查询给出带显式标注
  的纠正结果；精确名查询不带纠正标注（行为零变化）。

运行：python3 -m unittest tests.test_lookup_near_name_843b -v
联网用例可用 CN_LAW_VERIFY_LIVE=0 跳过。
"""

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402

LIVE = os.environ.get("CN_LAW_VERIFY_LIVE", "1") != "0"

# 842 开发集 lookup 错别字子类 20 例：(解析出的法名, 期望法规全名)
TYPO_CASES = [
    ("民法点", "中华人民共和国民法典"),
    ("劳动合问法", "中华人民共和国劳动合同法"),
    ("刑罚", "中华人民共和国刑法"),
    ("销法", "中华人民共和国消费者权益保护法"),
    ("公司珐", "中华人民共和国公司法"),
    ("食品按全法", "中华人民共和国食品安全法"),
    ("民诉珐", "中华人民共和国民事诉讼法"),
    ("个人信息保户法", "中华人民共和国个人信息保护法"),
    ("道路交通按全法", "中华人民共和国道路交通安全法"),
    ("未成年人保户法", "中华人民共和国未成年人保护法"),
    ("治安管理处罚珐", "中华人民共和国治安管理处罚法"),
    ("著作全法", "中华人民共和国著作权法"),
    ("商标珐", "中华人民共和国商标法"),
    ("专利珐", "中华人民共和国专利法"),
    ("行诉珐", "中华人民共和国行政诉讼法"),
    ("刑诉珐", "中华人民共和国刑事诉讼法"),
    ("劳动珐", "中华人民共和国劳动法"),
    ("社会保险珐", "中华人民共和国社会保险法"),
    ("广告珐", "中华人民共和国广告法"),
    ("数据按全法", "中华人民共和国数据安全法"),
]


class TestNearNameOffline(unittest.TestCase):
    def test_typo_cases_all_corrected(self):
        for name, expect in TYPO_CASES:
            with self.subTest(name=name):
                kind, payload = srv._near_name_match(name)
                self.assertEqual(kind, "corrected")
                self.assertEqual(payload[0], expect)

    def test_fabricated_names_no_candidate(self):
        for name in ("火星移民法", "不存在的法", "合成生物安全法",
                     "个人数据信托法"):
            with self.subTest(name=name):
                kind, _payload = srv._near_name_match(name)
                self.assertEqual(kind, "none")

    def test_threshold_and_tie_with_fake_universe(self):
        saved = srv._NAME_FORMS_CACHE
        try:
            srv._NAME_FORMS_CACHE = {
                "甲法": {"甲法全称"}, "乙法": {"乙法全称"},
            }
            # 编辑距离并列、拼音也并列 → tie，交人挑
            kind, payload = srv._near_name_match("丙法")
            self.assertEqual(kind, "tie")
            self.assertEqual(set(payload), {"甲法全称", "乙法全称"})
            # 超阈值 → none（5 字名对 2 字形式距离 3 > 2）
            kind, _payload = srv._near_name_match("甲乙丙丁法")
            self.assertEqual(kind, "none")
            # 精确命中不进纠错（距离 0 的形式被排除；邻名须够远，
            # 否则精确名也会被邻名吸走——真实流程里精确名由解析层
            # 直接命中，根本到不了纠错函数）
            srv._NAME_FORMS_CACHE = {
                "甲法": {"甲法全称"}, "天地玄黄法": {"天地玄黄法全称"},
            }
            kind, _payload = srv._near_name_match("甲法")
            self.assertEqual(kind, "none")
            # 编辑距离并列、同音胜出 → corrected（销 xiao = 消 xiao，
            # 宪 xian 读音距离更远）
            srv._NAME_FORMS_CACHE = {
                "消法": {"消费者法全称"}, "宪法": {"宪法全称"},
            }
            kind, payload = srv._near_name_match("销法")
            self.assertEqual(kind, "corrected")
            self.assertEqual(payload[0], "消费者法全称")
        finally:
            srv._NAME_FORMS_CACHE = saved


class TestVerifyIsolation(unittest.TestCase):
    def test_verify_resolvers_have_no_correction(self):
        import inspect
        for fn in (srv.find_law, srv.snapshot_find_law,
                   srv.tool_verify_citation,
                   srv.tool_check_citations_in_text):
            self.assertNotIn("_near_name_match",
                             inspect.getsource(fn),
                             f"{fn.__name__} 不许引用近名纠错")

    @unittest.skipUnless(LIVE, "CN_LAW_VERIFY_LIVE=0 跳过联网用例")
    def test_verify_rejects_typo_names(self):
        # 硬标准：写错的法名在核验路径一律不得 verified 放行。
        # 「刑罚」会被 flk 包含匹配吸到标题含该词的批复上（0.5.0
        # 既有语义），existence 因此为 True，但 verdict 仍拒核实；
        # 其余错名 existence 直接为 False。
        for name, expect_exist in (("民法点", False), ("公司珐", False),
                                   ("刑罚", True)):
            with self.subTest(name=name):
                out = srv.tool_verify_citation(
                    {"law": name, "article": "第一条"})
                self.assertEqual(out["verdict"], "not_verified")
                self.assertEqual(out["checks"]["existence"]["ok"],
                                 expect_exist)


@unittest.skipUnless(LIVE, "CN_LAW_VERIFY_LIVE=0 跳过联网用例")
class TestLookupLive(unittest.TestCase):
    def test_lookup_corrects_typo_with_annotation(self):
        out = srv.tool_lookup({"query": "民法点第577条"})
        mc = out["machine_conclusion"]
        self.assertEqual(mc["law"], "中华人民共和国民法典")
        self.assertEqual(mc["article"], "第五百七十七条")
        self.assertEqual(out["input"]["name_corrected_from"], "民法点")
        self.assertTrue(any("已按近名纠正为" in n
                            for n in out["notes"]))

    def test_lookup_exact_name_no_correction(self):
        out = srv.tool_lookup({"query": "民法典577条"})
        self.assertEqual(out["machine_conclusion"]["law"],
                         "中华人民共和国民法典")
        self.assertIsNone(out["input"]["name_corrected_from"])


if __name__ == "__main__":
    unittest.main()
