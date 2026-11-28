#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""F9 同文异版歧义开发集运行器（实时官方源）。

题面取自官方源现场取回的条文原文（tests/repeal_ambiguity_dev.jsonl），
与 865/867 密封题集零重合；期望 verdict 由构造方式决定。
citation_text 为 null 的题：运行器现场取回定位版官方原文作引文。

运行：python3 tests/run_repeal_ambiguity_dev.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))
import cn_law_verify_server as srv  # noqa: E402


def main():
    path = os.path.join(ROOT, "tests", "repeal_ambiguity_dev.jsonl")
    cases = [json.loads(line) for line in open(path, encoding="utf-8")
             if line.strip()]
    ok = 0
    for c in cases:
        text = c.get("citation_text")
        if text is None:
            law, _m = srv.find_law(c["law"])
            text, _s, _n = srv.fetch_official_article(
                law, c["article"], None, cache={})
            if text is None:
                print(f"MISS {c['id']} [对照引文现场取回失败]")
                continue
        try:
            out = srv.tool_verify_citation(
                {"law": c["law"], "article": c["article"],
                 "citation_text": text},
                _cache={}, _excerpt_ok=bool(c.get("excerpt")))
        except Exception as e:
            print(f"MISS {c['id']} [异常 {type(e).__name__}: {e}]")
            continue
        got = out["verdict"]
        amb = (out.get("checks") or {}).get("version_ambiguity") is not None
        good = (got == c["expected_verdict"]
                and amb == bool(c.get("expect_ambiguity")))
        if good:
            ok += 1
            print(f"PASS {c['id']} [{c['law']}第{c['article']}条] "
                  f"got={got} ambiguity={amb}")
        else:
            print(f"MISS {c['id']} [{c['law']}第{c['article']}条] "
                  f"expected={c['expected_verdict']}/ambiguity="
                  f"{bool(c.get('expect_ambiguity'))} "
                  f"got={got}/ambiguity={amb}")
            chk = json.dumps(out.get("checks") or {}, ensure_ascii=False)
            print("     checks=" + chk[:400])
    print(f"同文异版歧义开发集：{ok}/{len(cases)}")
    return 0 if ok == len(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
