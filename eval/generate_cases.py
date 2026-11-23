#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""评测集生成器：从快照库的真实条文＋实时官方源派生 200 题核验用例。

题目全部来自真实数据，不编造「标准答案」：
- true：快照中的真实条文（法名＋条号＋官方原文）→ 期望 verified；
- text_mismatch：同法另一条的原文配到本条条号下 → 期望 not_verified；
- drift：超出该法最大条号的条号 → 期望 not_verified；
- fabricated：虚构法名（生成时逐个经官方库 find_law 验证确实不存在，
  撞到真实法规的候选直接剔除）→ 期望 not_verified；
- colloquial：用简称（民法典/刑法/…）＋真实条文 → 期望 verified；
- repealed：已废止法规（合同法/婚姻法）的真实旧条文 → 期望 not_verified；
- cross_law：甲法法名＋乙法条文（条号在两法中都存在）→ 期望 not_verified。

输出 eval/cases.jsonl；题数不足 200 时如实打印实际题数，不凑数。
"""

import json
import os
import random
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))
import cn_law_verify_server as srv  # noqa: E402

DB = os.path.join(ROOT, "data", "snapshot.db")
OUT = os.path.join(ROOT, "eval", "cases.jsonl")
random.seed(842)

CORE_LAWS = [
    "中华人民共和国民法典",
    "中华人民共和国刑法",
    "中华人民共和国劳动合同法",
    "中华人民共和国公司法",
    "中华人民共和国消费者权益保护法",
    "中华人民共和国民事诉讼法",
    "中华人民共和国行政处罚法",
    "中华人民共和国个人信息保护法",
]

FAKE_CANDIDATES = [
    "中华人民共和国太空旅游法", "中华人民共和国宠物保护法",
    "中华人民共和国电子游戏管理法", "中华人民共和国直播电商法",
    "中华人民共和国元宇宙法", "中华人民共和国月球开发法",
    "中华人民共和国虚拟财产法", "中华人民共和国网络主播管理法",
    "中华人民共和国外卖配送法", "中华人民共和国共享单车法",
    "中华人民共和国电竞法", "中华人民共和国短视频法",
    "中华人民共和国大数据法", "中华人民共和国机器人法",
    "中华人民共和国脑机接口法", "中华人民共和国数字货币法",
    "中华人民共和国气候变化应对法", "中华人民共和国深海采矿法",
    "中华人民共和国基因编辑法", "中华人民共和国量子通信法",
    "中华人民共和国飞行汽车法", "中华人民共和国数字遗产法",
    "中华人民共和国合成生物学法", "中华人民共和国太空资源法",
    "中华人民共和国预付式消费法", "中华人民共和国快递包装法",
    "中华人民共和国无人机驾驶法", "中华人民共和国人工智能生成内容法",
]

COLLOQUIAL = [("民法典", "中华人民共和国民法典", 5),
              ("刑法", "中华人民共和国刑法", 3),
              ("公司法", "中华人民共和国公司法", 3),
              ("劳动合同法", "中华人民共和国劳动合同法", 2),
              ("消费者权益保护法", "中华人民共和国消费者权益保护法", 2)]

REPEALED = [("中华人民共和国合同法", 6), ("中华人民共和国婚姻法", 4)]


def load_snapshot():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    laws = {}
    for row in conn.execute("SELECT * FROM laws"):
        arts = [dict(r) for r in conn.execute(
            "SELECT * FROM articles WHERE bbbs=? ORDER BY num, sub",
            (row["bbbs"],))]
        laws[row["title"]] = {"law": dict(row), "articles": arts}
    conn.close()
    return laws


def usable(articles):
    return [a for a in articles if len(a["text"]) >= 10 and not a["sub"]]


def main():
    snap = load_snapshot()
    missing = [t for t in CORE_LAWS if t not in snap]
    if missing:
        print(f"快照缺以下法规，先跑构建器：{missing}")
        return 1
    cases = []

    def add(category, law, article, text, expected, note=""):
        cases.append({
            "id": f"{category}-{len([c for c in cases if c['category'] == category]) + 1:03d}",
            "category": category, "law": law, "article": article,
            "citation_text": text, "expected_verdict": expected,
            "note": note})

    # ---- true（80）：8 部法各 10 条
    for title in CORE_LAWS:
        pool = usable(snap[title]["articles"])
        for a in random.sample(pool, 10):
            add("true", title, a["heading"], a["text"], "verified",
                "快照真实条文")
    # ---- text_mismatch（40）：8 部法各 5 条，配同法另一条原文
    for title in CORE_LAWS:
        pool = usable(snap[title]["articles"])
        picks = random.sample(pool, 5)
        for a in picks:
            others = [b for b in pool if b["num"] != a["num"]]
            b = random.choice(others)
            add("text_mismatch", title, a["heading"], b["text"],
                "not_verified", f"文本实为{b['heading']}")
    # ---- drift（30）：条号超出该法最大条号
    drift_plan = [10, 6, 4, 4, 2, 2, 1, 1]
    for title, n in zip(CORE_LAWS, drift_plan):
        max_num = max(a["num"] for a in snap[title]["articles"])
        for i in range(n):
            num = max_num + random.randint(7, 300) + i
            add("drift", title, srv.article_heading(num), None,
                "not_verified", f"该法最大条号 {max_num}")
    # ---- fabricated（20）：逐个验证不存在
    got = 0
    for fake in FAKE_CANDIDATES:
        if got >= 20:
            break
        found, _m = srv.find_law(fake)
        if found is not None:
            print(f"候选虚构法名《{fake}》在官方库有定位结果"
                  f"（《{found['title']}》），剔除")
            continue
        add("fabricated", fake, "第一条", None, "not_verified",
            "生成时经官方库核实不存在")
        got += 1
    print(f"fabricated 实际取得 {got} 题")
    # ---- colloquial（15）
    for short, full, n in COLLOQUIAL:
        pool = usable(snap[full]["articles"])
        for a in random.sample(pool, n):
            add("colloquial", short, a["heading"], a["text"], "verified",
                f"简称，对应《{full}》")
    # ---- repealed（10）：实时取已废止法规真实条文
    for title, n in REPEALED:
        law, _m = srv.find_law(title)
        if law is None:
            print(f"已废止法规《{title}》未定位到，跳过")
            continue
        paragraphs = srv.fetch_law_docx_paragraphs(law["id"])
        arts = [a for a in srv.split_articles(paragraphs)
                if len(a["text"]) >= 10 and not a["sub"]]
        for a in random.sample(arts, min(n, len(arts))):
            add("repealed", title, a["heading"], a["text"], "not_verified",
                f"法规效力状态：{law['status']}")
    # ---- cross_law（5）
    pairs = [("中华人民共和国民法典", "中华人民共和国刑法"),
             ("中华人民共和国刑法", "中华人民共和国民法典"),
             ("中华人民共和国公司法", "中华人民共和国民法典"),
             ("中华人民共和国劳动合同法", "中华人民共和国民法典"),
             ("中华人民共和国民事诉讼法", "中华人民共和国刑事诉讼法")
             if "中华人民共和国刑事诉讼法" in snap else
             ("中华人民共和国民事诉讼法", "中华人民共和国民法典")]
    for name_a, name_b in pairs:
        pool_b = usable(snap[name_b]["articles"])
        nums_a = {a["num"] for a in snap[name_a]["articles"]}
        common = [b for b in pool_b if b["num"] in nums_a]
        if not common:
            continue
        b = random.choice(common)
        add("cross_law", name_a, b["heading"], b["text"], "not_verified",
            f"文本实为《{name_b}》{b['heading']}")

    with open(OUT, "w", encoding="utf-8") as f:
        for c in cases:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")
    from collections import Counter
    print("分类计数：", dict(Counter(c["category"] for c in cases)))
    print(f"总题数：{len(cases)}（目标 200）→ {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
