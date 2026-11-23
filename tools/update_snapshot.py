#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 快照增量更新器（857 新增，离线运行，非 MCP 工具）。

与全量构建（build_snapshot.py）的分工：构建器负责「从无到有」，
本工具负责「保鲜」——
1. 逐部比对：对库内每部法规拉一次详情页元数据（效力代码/公布日/
   施行日），与库内记录不一致才整部重建（修订法、新效力状态都走
   这条路），一致的零下载、不重切；
2. 新法并入：枚举清单（data/enumeration.jsonl）与库内 bbbs 做差集，
   新公布的法规按 --max-new 限量并入（默认 20 部/次，防止单次
   运行失控），剩余下轮继续。

纪律与构建器同源：串行＋间隔（默认 2 秒）、单部失败不中断、
结果如实落盘（data/update_log.jsonl 逐轮追加＋结尾覆盖 meta）。

用法：
    python3 tools/update_snapshot.py                 # 全量比对＋并入新法
    python3 tools/update_snapshot.py --max-new 0     # 只比对、不并入
    python3 tools/update_snapshot.py --limit-checks 50   # 只比对前 50 部（巡检）
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "server"))

import build_snapshot as bs  # noqa: E402
import cn_law_verify_server as srv  # noqa: E402

DEFAULT_DB = os.path.join(ROOT, "data", "snapshot.db")
DEFAULT_IDS = os.path.join(ROOT, "data", "enumeration.jsonl")
DEFAULT_LOG = os.path.join(ROOT, "data", "update_log.jsonl")


def main() -> int:
    ap = argparse.ArgumentParser(description="cn-law-verify 快照增量更新器")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--ids", default=DEFAULT_IDS,
                    help="枚举清单 JSONL（新法差集来源）")
    ap.add_argument("--max-new", type=int, default=20,
                    help="本轮最多并入的新法部数（默认 20，0=不并入）")
    ap.add_argument("--limit-checks", type=int, default=0,
                    help="只比对前 N 部（0=全部）")
    ap.add_argument("--delay", type=float, default=2.0)
    ap.add_argument("--log", default=DEFAULT_LOG)
    args = ap.parse_args()

    conn = bs.init_db(args.db)
    rows = conn.execute(
        "SELECT bbbs, title, status_code, promulgated, effective_from "
        "FROM laws ORDER BY title").fetchall()
    if args.limit_checks:
        rows = rows[:args.limit_checks]
    print(f"增量更新：库内共比对 {len(rows)} 部；新法并入上限 "
          f"{args.max_new} 部", flush=True)

    checked = changed = rebuilt = check_failed = 0
    changes = []
    for bbbs, title, code, gbrq, sxrq in rows:
        try:
            details = srv.fetch_law_details(bbbs)
        except Exception as e:  # 单部失败不中断
            check_failed += 1
            changes.append({"bbbs": bbbs, "title": title,
                            "error": f"{type(e).__name__}: {e}"})
            continue
        checked += 1
        _zh, _en, new_code = srv.map_status(details.get("sxx"))
        new_gbrq = details.get("gbrq") or ""
        new_sxrq = details.get("sxrq") or ""
        if (new_code, new_gbrq, new_sxrq) != (code, gbrq or "", sxrq or ""):
            changed += 1
            entry = bs.build_one_by_id(conn, bbbs, title, args.delay)
            if entry["ok"]:
                rebuilt += 1
            changes.append({
                "bbbs": bbbs, "title": title,
                "from": {"status_code": code, "promulgated": gbrq,
                         "effective_from": sxrq},
                "to": {"status_code": new_code, "promulgated": new_gbrq,
                       "effective_from": new_sxrq},
                "rebuilt": entry["ok"],
                "error": entry.get("error"),
            })
            print(f"  变化：《{title}》效力/日期有变 → 重建 "
                  f"{'成功' if entry['ok'] else '失败'}", flush=True)
        if args.delay:
            time.sleep(args.delay)

    added = add_failed = 0
    new_laws = []
    if args.max_new > 0 and os.path.exists(args.ids):
        enum_rows = [json.loads(line) for line in
                     open(args.ids, encoding="utf-8") if line.strip()]
        in_db = bs.existing_ids(conn)
        fresh = [r for r in enum_rows if r["bbbs"] not in in_db]
        for row in fresh[:args.max_new]:
            entry = bs.build_one_by_id(conn, row["bbbs"], row["title"],
                                       args.delay)
            if entry["ok"]:
                added += 1
                new_laws.append(row["title"])
            else:
                add_failed += 1

    bs.write_meta(conn, bs.rebuild_coverage(conn))
    total_laws = conn.execute("SELECT COUNT(*) FROM laws").fetchone()[0]
    total_arts = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    summary = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "checked": checked, "check_failed": check_failed,
        "changed": changed, "rebuilt": rebuilt,
        "new_added": added, "new_failed": add_failed,
        "new_laws": new_laws, "changes": changes,
        "library": {"laws": total_laws, "articles": total_arts},
    }
    with open(args.log, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(summary, ensure_ascii=False) + "\n")
    print(f"完成：比对 {checked} 部（失败 {check_failed}），变化 {changed} 部"
          f"（重建成功 {rebuilt}），新并入 {added} 部（失败 {add_failed}）；"
          f"库内共 {total_laws} 部 / {total_arts} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
