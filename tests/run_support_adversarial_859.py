#!/usr/bin/env python3
"""859 对抗集运行器：32 题全新构造的 check_support 对抗题。

与 857 金标的区别：这批题的句式刻意避开金标原题，专打 v2 的
过拟合面（方向对调、双重条件、序号污染、主体宾语误绑等）。
已知未检出（多档并存型方向对调，词面启发式的结构性边界，
见 RESULT_859 最没把握清单）单列 KNOWN_MISS：命中它们不算
回归，出现清单外的新 miss 才返回 1。
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "server"))
import cn_law_verify_server as srv  # noqa: E402

KNOWN_MISS = {"a859-b01", "a859-b02"}


def main() -> int:
    rows = [json.loads(line) for line in
            (HERE / "support_adversarial_859.jsonl")
            .read_text(encoding="utf-8").splitlines() if line.strip()]
    ok, misses, known = 0, [], []
    for row in rows:
        out = srv.tool_check_support({
            "claim": row["claim"], "citation_text": row["citation_text"]})
        got = out["verdict"]
        if got == row["expected"]:
            ok += 1
        elif row["id"] in KNOWN_MISS:
            known.append(row["id"])
        else:
            misses.append((row["id"], row["expected"], got))
    print(f"859 对抗集：{ok}/{len(rows)}"
          f"（已知未检出 {len(known)}：{', '.join(known) or '无'}）")
    for m in misses:
        print(f"  NEW MISS {m[0]} expected={m[1]} got={m[2]}")
    return 1 if misses else 0


if __name__ == "__main__":
    raise SystemExit(main())
