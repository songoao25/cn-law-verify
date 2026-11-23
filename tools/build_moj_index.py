#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 司法部行政法规库标题索引构建器（859 新增，离线运行）。

背景（859 对抗审计实测）：moj 三个检索端点的 Query 参数当前不生效，
恒返回全库按日期升序的第一页——857 的 moj 兜底因此只能命中第一页
约 10 部法规。但同一端点接受 PageIndex 分页（857 未探明），全库可
逐页遍历。本工具把全库（标题＋LawID＋列表页可见的效力/日期）抓成
本地索引 data/moj_index.json，server 的 moj 兜底与 moj 标题检索
改走索引：精确标题直达详情页、标题子串检索覆盖全库。

纪律：串行＋间隔（默认 1 秒）、单页失败重试后跳过并如实记录、
结果落盘（含构建时间与覆盖数）。索引只是「标题→ID」电话簿，
效力与原文仍以详情页实时取数为准，不冒充官方数据快照。

用法：
    python3 tools/build_moj_index.py                 # 全量遍历
    python3 tools/build_moj_index.py --max-pages 5   # 只抓前 5 页（试跑）
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402

DEFAULT_OUT = os.path.join(ROOT, "data", "moj_index.json")
PAGE_SIZE_HINT = 10


def fetch_page(page_index, query=""):
    params = {"SiteID": "122", "Query": query, "Type": "1",
              "PageIndex": str(page_index)}
    url = (f"{srv.MOJ_BASE}/SearchTitleFront?"
           + urllib.parse.urlencode(params))
    html_text = srv._fetch_alt_text(url, "司法部行政法规库列表",
                                    srv.MOJ_BASE)
    return srv._moj_parse_list(html_text), html_text


def main() -> int:
    ap = argparse.ArgumentParser(description="moj 标题索引构建器")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()

    items, seen = [], set()
    failed_pages = []
    total_hint = None
    page = 1
    while page <= args.max_pages:
        try:
            rows, html_text = fetch_page(page)
        except Exception as e:  # 单页失败不中断整轮
            failed_pages.append({"page": page,
                                 "error": f"{type(e).__name__}: {e}"})
            print(f"  第 {page} 页失败，跳过：{e}", flush=True)
            page += 1
            continue
        if total_hint is None:
            import re
            # 列表页隐藏域 law-total 是官方口径的「现行有效」部数
            # （859 实测 603）；分页遍历的实际行数更多，因为同一
            # 法规的历史版本也占行（同名多 ID）。两者都如实记录。
            m = re.search(r'id="law-total"[^>]*value="([\d,]+)"',
                          html_text)
            if m:
                total_hint = int(m.group(1).replace(",", ""))
        new = 0
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            items.append({"id": r["id"], "title": r["title"],
                          "status": r.get("status", ""),
                          "promulgated": r.get("promulgated", "")})
            new += 1
        print(f"  第 {page} 页：{len(rows)} 条（新增 {new}，累计 "
              f"{len(items)}）", flush=True)
        if not rows or new == 0:
            break
        page += 1
        if args.delay:
            time.sleep(args.delay)

    payload = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source": srv.MOJ_SOURCE_LABEL,
        "total_hint": total_hint,
        "count": len(items),
        "failed_pages": failed_pages,
        "items": items,
    }
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    print(f"完成：索引 {len(items)} 行（含同名历史版本；官方现行有效"
          f"口径 {total_hint} 部），失败页 {len(failed_pages)}，"
          f"已写入 {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
