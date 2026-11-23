#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""评测运行器：逐题调 verify_citation，与期望结论比对，出成绩单。

为控制总时长，运行器在本进程内给 find_law 与 DOCX 下载加了一层
进程内缓存（同一部法多题复用一次检索/下载）；server 本身不缓存，
评测缓存不改变任何单题的核验逻辑与判定路径。

输出 eval/RESULTS.md（逐类准确率＋失败题明细），原始结果存
eval/last_run.json。
"""

import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))
import cn_law_verify_server as srv  # noqa: E402

CASES = os.path.join(ROOT, "eval", "cases.jsonl")
RESULTS_MD = os.path.join(ROOT, "eval", "RESULTS.md")
LAST_RUN = os.path.join(ROOT, "eval", "last_run.json")

_find_cache = {}
_orig_find = srv.find_law


def _find_cached(name):
    if name not in _find_cache:
        _find_cache[name] = _orig_find(name)
    return _find_cache[name]


_docx_cache = {}
_orig_docx = srv.fetch_law_docx_paragraphs


def _docx_cached(bbbs):
    if bbbs not in _docx_cache:
        try:
            _docx_cache[bbbs] = _orig_docx(bbbs)
        except srv.SourceError as e:
            _docx_cache[bbbs] = e
    val = _docx_cache[bbbs]
    if isinstance(val, Exception):
        raise val
    return val


srv.find_law = _find_cached
srv.fetch_law_docx_paragraphs = _docx_cached


def main():
    cases = [json.loads(line) for line in
             open(CASES, encoding="utf-8") if line.strip()]
    print(f"共 {len(cases)} 题，开始评测…", flush=True)
    results = []
    started = time.time()
    for i, case in enumerate(cases, 1):
        args = {"law": case["law"], "article": case["article"]}
        if case.get("citation_text"):
            args["citation_text"] = case["citation_text"]
        t0 = time.time()
        try:
            out = srv.tool_verify_citation(args)
            actual = out["verdict"]
            detail = ""
            if actual != case["expected_verdict"]:
                checks = out.get("checks") or {}
                detail = "; ".join(
                    f"{k}={v.get('ok')}" for k, v in checks.items()
                    if isinstance(v, dict))
        except Exception as e:
            actual = f"error:{type(e).__name__}"
            detail = str(e)[:200]
        ok = actual == case["expected_verdict"]
        results.append({**case, "actual_verdict": actual, "pass": ok,
                        "detail": detail,
                        "seconds": round(time.time() - t0, 2)})
        if i % 20 == 0 or not ok:
            print(f"[{i}/{len(cases)}] {case['id']} 期望 "
                  f"{case['expected_verdict']} 实际 {actual} "
                  f"{'✔' if ok else '✘ ' + detail}", flush=True)
    elapsed = round(time.time() - started, 1)
    passed = sum(1 for r in results if r["pass"])
    by_cat = defaultdict(list)
    for r in results:
        by_cat[r["category"]].append(r)
    lines = [
        "# cn-law-verify 引注核验评测基线成绩单",
        "",
        f"- 运行时间：{datetime.now(timezone.utc).isoformat()}",
        f"- server 版本：{srv.SERVER_VERSION}",
        f"- 题集：eval/cases.jsonl（{len(cases)} 题，生成方式见 generate_cases.py）",
        f"- 总耗时：{elapsed} 秒（进程内缓存检索与 DOCX 下载，未改核验逻辑）",
        "",
        f"## 总成绩：{passed}/{len(cases)} = {passed / len(cases):.1%}",
        "",
        "| 类别 | 题数 | 通过 | 准确率 |",
        "|---|---|---|---|",
    ]
    for cat in sorted(by_cat):
        rs = by_cat[cat]
        p = sum(1 for r in rs if r["pass"])
        lines.append(f"| {cat} | {len(rs)} | {p} | {p / len(rs):.1%} |")
    fails = [r for r in results if not r["pass"]]
    lines += ["", f"## 失败题明细（{len(fails)} 题）", ""]
    if fails:
        for r in fails:
            lines.append(f"- `{r['id']}` 《{r['law']}》{r['article']}："
                         f"期望 {r['expected_verdict']}，实际 "
                         f"{r['actual_verdict']}。{r['detail']}。"
                         f"备注：{r.get('note', '')}")
    else:
        lines.append("（无）")
    lines += ["", "说明：本成绩是本评测集上的基线，不代表对任意引注的"
                  "准确率承诺；题目由真实条文派生（真引注/文本错位/"
                  "条号漂移/编造法名/简称/已废止/跨法错配七类）。"]
    with open(RESULTS_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    with open(LAST_RUN, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
