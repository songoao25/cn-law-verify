#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 快照扩量枚举器（离线运行，非 MCP 工具）。

分页枚举国家法律法规数据库（flk）中指定分类、指定效力状态的全部法规，
把 (bbbs, title, flxz, status_code) 写入 JSONL 清单，供
tools/build_snapshot.py --ids 按 id 直接建库（避免按名二次检索）。

分类代码（2026-11-23 实测自官方检索接口 flfgCodeId ↔ flxz 对应）：
    100 宪法 | 110/120/130/140/150/160/170 法律 | 180 法律解释
    190 有关法律问题和重大问题的决定 | 210 行政法规 | 220 监察法规
    305 法规性决定 | 320/330/340 司法解释
    230/260/270/290/300 地方法规（量级 1.5 万+，默认不枚举）

纪律：串行＋页间间隔（默认 1 秒），单页失败重试由 server 的 _request
承担；枚举只读官方检索接口，不下载全文。

用法：
    python3 tools/enumerate_flk.py                 # 国家层面主体分类
    python3 tools/enumerate_flk.py --codes 210     # 只枚举行政法规
    python3 tools/enumerate_flk.py --status 3 1    # 有效+已废止
    python3 tools/enumerate_flk.py --out data/enumeration.jsonl
"""

import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402

# 国家层面主体分类（本轮扩量范围）；地方法规代码单列，默认不含
NATIONAL_CODES = [100, 110, 120, 130, 140, 150, 160, 170, 180, 190,
                  210, 220, 305, 320, 330, 340]
LOCAL_CODES = [230, 260, 270, 290, 300]

DEFAULT_OUT = os.path.join(ROOT, "data", "enumeration.jsonl")


def fetch_page(code, page_num, page_size, status):
    body = {
        "searchContent": "", "searchType": 2, "searchRange": 1,
        "pageNum": page_num, "pageSize": page_size,
        "sxx": list(status), "gbrq": [], "sxrq": [],
        "flfgCodeId": [code], "zdjgCodeId": [],
    }
    res = srv._http_json("/law-search/search/list", body)
    if not isinstance(res, dict) or "rows" not in res:
        raise srv.SourceError(f"枚举响应结构异常：{str(res)[:200]}")
    return int(res.get("total") or 0), res.get("rows") or []


def fetch_page_robust(code, page_num, page_size, status, attempts=8):
    """页级重试：本机出口对官方响应常静默截断，单页失败不许拖垮全程。"""
    last = None
    for i in range(attempts):
        try:
            return fetch_page(code, page_num, page_size, status)
        except srv.SourceError as e:
            last = e
            print(f"  分类 {code} 第 {page_num} 页第 {i + 1} 次失败：{e}；"
                  "稍后重试", flush=True)
            time.sleep(3.0 * (i + 1))
    raise last


def enumerate_code(code, status, page_size, delay, seen, out_f):
    total, rows = fetch_page_robust(code, 1, page_size, status)
    pages = (total + page_size - 1) // page_size if total else 0
    print(f"分类 {code}：共 {total} 条，{pages} 页", flush=True)
    n = 0
    for pn in range(1, pages + 1):
        if pn > 1:
            _t, rows = fetch_page_robust(code, pn, page_size, status)
            if delay:
                time.sleep(delay)
        for r in rows:
            bbbs = r.get("bbbs") or ""
            if not bbbs or bbbs in seen:
                continue
            seen.add(bbbs)
            out_f.write(json.dumps({
                "bbbs": bbbs,
                "title": srv.clean_title(r.get("title")),
                "flxz": r.get("flxz") or "",
                "status_code": r.get("sxx"),
                "code": code,
            }, ensure_ascii=False) + "\n")
            n += 1
        out_f.flush()
    print(f"分类 {code}：写入 {n} 条", flush=True)
    return n


def main():
    ap = argparse.ArgumentParser(description="flk 分类枚举器")
    ap.add_argument("--codes", type=int, nargs="*", default=None,
                    help="分类代码（默认国家层面主体分类）")
    ap.add_argument("--status", type=int, nargs="*", default=[3],
                    help="效力状态代码（默认 3=有效）")
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--delay", type=float, default=1.0,
                    help="页间间隔秒数（默认 1）")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    codes = args.codes if args.codes is not None else NATIONAL_CODES
    seen = set()
    if os.path.exists(args.out):
        with open(args.out, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen.add(json.loads(line)["bbbs"])
        print(f"已有清单 {len(seen)} 条，续跑去重", flush=True)
    total = 0
    failed_codes = []
    with open(args.out, "a", encoding="utf-8") as out_f:
        for code in codes:
            try:
                total += enumerate_code(code, args.status, args.page_size,
                                        args.delay, seen, out_f)
            except srv.SourceError as e:
                # 单个分类失败不中断全程；续跑（重跑本脚本）会按 bbbs
                # 去重补齐缺页
                failed_codes.append(code)
                print(f"分类 {code} 本轮失败，跳过续跑：{e}", flush=True)
    if failed_codes:
        print(f"注意：分类 {failed_codes} 本轮未完成，重跑本脚本可续补")
    print(f"完成：本轮新增 {total} 条，清单共 {len(seen)} 条 → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
