#!/usr/bin/env python3
"""check_support v5 开发集运行器（2026-11-26 新建，24 题）。

题面取自 871 附表重建的 22 个失败主张（引文为官方源现场取回的条文原文）＋5 个忠实转述对照＋3 个金标回归；
与金标集、对抗集、865/867 密封题集零重合；期望 verdict 由构造方式决定。
用法：python3 tests/run_support_v4_dev.py
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "server"))
import cn_law_verify_server as srv  # noqa: E402


def main() -> int:
    rows = [json.loads(line) for line in
            (HERE / "support_v5_dev.jsonl").read_text(encoding="utf-8").splitlines()
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
                           out.get("reasons", [])[:1],
                           out.get("clause_coverage", [])))
    total_ok = sum(v[0] for v in cats.values())
    print(f"v5 开发集：{total_ok}/{len(rows)}")
    for cat in sorted(cats):
        ok, n = cats[cat]
        print(f"  {cat:18s} {ok}/{n}")
    for m in misses:
        print(f"  MISS {m[0]} [{m[1]}] expected={m[2]} got={m[3]}")
        print(f"       reasons={m[4]} clause_coverage={m[5]}")
    return 0 if total_ok == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
