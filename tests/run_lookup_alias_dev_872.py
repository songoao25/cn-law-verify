#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""简称开发集 872运行器（2026-11-27，积压②）：tests/lookup_alias_dev_872.jsonl。

每题经 tool_lookup 真实调用（含 flk 实时腿），断言定位到的法规全名。
用法：python3 tests/run_lookup_alias_dev.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402


def main():
    path = os.path.join(ROOT, "tests", "lookup_alias_dev_872.jsonl")
    cases = [json.loads(line) for line in open(path, encoding="utf-8")
             if line.strip()]
    ok = 0
    for case in cases:
        try:
            out = srv.tool_lookup({"query": case["query"]})
            got = out["machine_conclusion"]["law"]
        except Exception as exc:  # noqa: BLE001
            got = f"ERROR {type(exc).__name__}: {exc}"
        good = got == case["expected_law"]
        ok += good
        print(f"{'PASS' if good else 'MISS'} {case['id']} "
              f"[{case['query']}] got={got}")
    print(f"简称开发集 872：{ok}/{len(cases)}")
    return 0 if ok == len(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
