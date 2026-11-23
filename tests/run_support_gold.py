#!/usr/bin/env python3
"""check_support 金标集运行器（857 补强）。

用法：python3 tests/run_support_gold.py
逐题调用 tool_check_support，比对期望 verdict，输出分类命中与错题清单。
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "server"))
import cn_law_verify_server as srv  # noqa: E402


def main() -> int:
    rows = [json.loads(line) for line in
            (HERE / "support_gold.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()]
    cats: dict[str, list[int]] = {}
    misses = []
    for row in rows:
        out = srv.tool_check_support({
            "claim": row["claim"], "citation_text": row["citation_text"]})
        got = out["verdict"]
        ok = got == row["expected"]
        cats.setdefault(row["category"], [0, 0])
        cats[row["category"]][1] += 1
        if ok:
            cats[row["category"]][0] += 1
        else:
            misses.append((row["id"], row["category"], row["expected"], got,
                           out.get("signals", [])[:2]))
    total_ok = sum(v[0] for v in cats.values())
    print(f"method={srv.tool_check_support.__module__} total {total_ok}/{len(rows)}")
    for cat in sorted(cats):
        ok, n = cats[cat]
        print(f"  {cat:16s} {ok}/{n}")
    for m in misses:
        print(f"  MISS {m[0]} [{m[1]}] expected={m[2]} got={m[3]} signals={m[4]}")
    return 0 if total_ok == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
