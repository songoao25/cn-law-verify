#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cn-law-verify 快照库构建器（离线运行，非 MCP 工具）。

从国家法律法规数据库官方源逐部取回法规全文（官方 DOCX），切成条文
写入 SQLite 快照库（data/snapshot.db），供 get_article / search_articles
的快照通道使用。

纪律：
- 串行＋限速：每部法规之间固定间隔（默认 2 秒，可调），单部失败不中断
  全程，失败原因如实记入覆盖清单；
- 断点续传：已入库的法规（按 bbbs）默认跳过，--refresh 才重取；
- 只收录定位结果标题与目标法名完全一致的法规，简称顶替不入库；
- TLS 纪律与 server 同源（复用 server 的 HTTP 与解析实现）。
- 全量 2.6 万部不在一次构建内：本脚本按内置优先清单逐批扩，
  覆盖范围如实写进库内 meta 与仓库 README。

用法：
    python3 tools/build_snapshot.py                 # 按优先清单构建/续跑
    python3 tools/build_snapshot.py --refresh       # 全部重取
    python3 tools/build_snapshot.py --law 中华人民共和国商标法
    python3 tools/build_snapshot.py --list          # 只打印当前覆盖
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "server"))

import cn_law_verify_server as srv  # noqa: E402

DEFAULT_DB = os.path.join(ROOT, "data", "snapshot.db")

# 首批优先清单：核心法典＋高频法律（用户最可能引用的范围）。
# 全量扩建按同一清单机制分批追加，不在本脚本一次跑完 2.6 万部。
PRIORITY_LAWS = [
    # 宪法在官方库中的著录标题带年份后缀，按官方标题收录
    "中华人民共和国宪法（1982年）",
    "中华人民共和国民法典",
    "中华人民共和国刑法",
    "中华人民共和国刑事诉讼法",
    "中华人民共和国民事诉讼法",
    "中华人民共和国行政诉讼法",
    "中华人民共和国行政处罚法",
    "中华人民共和国行政许可法",
    "中华人民共和国行政强制法",
    "中华人民共和国国家赔偿法",
    "中华人民共和国立法法",
    "中华人民共和国公司法",
    "中华人民共和国合伙企业法",
    "中华人民共和国劳动合同法",
    "中华人民共和国劳动法",
    "中华人民共和国社会保险法",
    "中华人民共和国消费者权益保护法",
    "中华人民共和国产品质量法",
    "中华人民共和国食品安全法",
    "中华人民共和国道路交通安全法",
    "中华人民共和国治安管理处罚法",
    "中华人民共和国个人信息保护法",
    "中华人民共和国数据安全法",
    "中华人民共和国网络安全法",
    "中华人民共和国著作权法",
    "中华人民共和国商标法",
    "中华人民共和国专利法",
    "中华人民共和国未成年人保护法",
    "中华人民共和国妇女权益保障法",
    "中华人民共和国环境保护法",
    "中华人民共和国反不正当竞争法",
    "中华人民共和国广告法",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS laws(
    bbbs TEXT PRIMARY KEY, title TEXT, authority TEXT, category TEXT,
    promulgated TEXT, effective_from TEXT, status_code INTEGER,
    source_url TEXT, toc_json TEXT, article_count INTEGER, fetched_at TEXT);
CREATE TABLE IF NOT EXISTS articles(
    bbbs TEXT, num INTEGER, sub INTEGER, heading TEXT, text TEXT,
    PRIMARY KEY(bbbs, num, sub));
CREATE INDEX IF NOT EXISTS idx_articles_bbbs ON articles(bbbs);
"""


def init_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def existing_ids(conn):
    return {r[0] for r in conn.execute("SELECT bbbs FROM laws")}


def build_one(conn, title, delay, attempts=4):
    """构建一部法规 → coverage 条目 dict。失败不抛异常，原因入条目。

    官方源经本机出口时大响应常被截断（server 内部已重试 5 次），
    构建器再做整部重试 attempts 次（间隔 8 秒），提高单部成功率。
    """
    entry = {"title": title, "ok": False}
    for attempt in range(attempts):
        entry = _build_one_once(conn, title)
        if entry["ok"]:
            break
        if attempt + 1 < attempts:
            print(f"    第 {attempt + 1} 次失败：{entry.get('error')}；"
                  "8 秒后整部重试", flush=True)
            time.sleep(8)
    if delay:
        time.sleep(delay)
    return entry


def _build_one_once(conn, title):
    entry = {"title": title, "ok": False}
    try:
        law, match = srv.find_law(title)
        if law is None:
            entry["error"] = "官方库未检索到"
            return entry
        if law["title"] != title:
            entry["error"] = (f"定位结果标题不一致（得到《{law['title']}》，"
                              "匹配方式 " + str(match) + "），为防张冠李戴不入库")
            return entry
        details = srv.fetch_law_details(law["id"])
        flat = srv.flatten_toc(details.get("content") or {})
        paragraphs = srv.fetch_law_docx_paragraphs(law["id"])
        articles = srv.split_articles(paragraphs)
        if not articles:
            entry["error"] = "官方全文未切出任何条文"
            return entry
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO laws VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (law["id"], law["title"], law["authority"], law["category"],
             law["promulgated"], law["effective_from"], law["status_code"],
             law["source_url"], json.dumps(flat, ensure_ascii=False),
             len(articles), now))
        conn.execute("DELETE FROM articles WHERE bbbs=?", (law["id"],))
        conn.executemany(
            "INSERT OR REPLACE INTO articles VALUES (?,?,?,?,?)",
            [(law["id"], a["num"], a["sub"] or 0, a["heading"], a["text"])
             for a in articles])
        conn.commit()
        entry.update({"ok": True, "id": law["id"],
                      "status": law["status"],
                      "status_code": law["status_code"],
                      "article_count": len(articles),
                      "toc_article_count": srv.toc_article_count(flat),
                      "fetched_at": now})
    except Exception as e:  # 单部失败不中断全程
        entry["error"] = f"{type(e).__name__}: {e}"
    return entry


def write_meta(conn, coverage):
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('built_at', ?)",
                 (datetime.now(timezone.utc).isoformat(),))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('coverage', ?)",
                 (json.dumps(coverage, ensure_ascii=False),))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('builder', ?)",
                 ("tools/build_snapshot.py",))
    conn.commit()


def print_coverage(conn):
    info_rows = conn.execute(
        "SELECT title, status_code, article_count FROM laws ORDER BY title"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"快照库现收录 {len(info_rows)} 部法规、{total} 条条文：")
    for title, code, count in info_rows:
        zh, _en, _c = srv.map_status(code)
        print(f"  《{title}》 {zh} {count} 条")


def main():
    ap = argparse.ArgumentParser(description="cn-law-verify 快照库构建器")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--refresh", action="store_true", help="已入库也重取")
    ap.add_argument("--law", action="append", default=[],
                    help="只构建指定法规（可多次）")
    ap.add_argument("--delay", type=float, default=2.0,
                    help="每部法规之间的间隔秒数（默认 2）")
    ap.add_argument("--list", action="store_true", help="只打印当前覆盖")
    args = ap.parse_args()

    conn = init_db(args.db)
    if args.list:
        print_coverage(conn)
        return 0
    targets = args.law or PRIORITY_LAWS
    # 断点续传以 laws 表为准（不依赖 meta 里可能过期的 coverage）
    in_db = {r[0]: r[1] for r in conn.execute(
        "SELECT title, bbbs FROM laws")}
    results = []
    failures = []
    for i, title in enumerate(targets, 1):
        if title in in_db and not args.refresh:
            print(f"[{i}/{len(targets)}] 《{title}》 已入库，跳过"
                  "（--refresh 重取）")
            continue
        print(f"[{i}/{len(targets)}] 《{title}》 构建中…", flush=True)
        entry = build_one(conn, title, args.delay)
        if entry["ok"]:
            print(f"    ✔ {entry['article_count']} 条"
                  f"（目录计 {entry['toc_article_count']} 条）"
                  f" 效力：{entry['status']}")
            results.append(entry)
        else:
            print(f"    ✘ {entry.get('error')}")
            failures.append(entry)
    # 覆盖清单如实重建：库内全部法规（ok）＋本轮失败条目（带原因）
    coverage = []
    for row in conn.execute(
            "SELECT bbbs, title, status_code, article_count, fetched_at "
            "FROM laws"):
        zh, _en, _c = srv.map_status(row[2])
        coverage.append({"title": row[1], "ok": True, "id": row[0],
                         "status": zh, "status_code": row[2],
                         "article_count": row[3], "fetched_at": row[4]})
    coverage.extend(failures)
    write_meta(conn, coverage)
    ok = sum(1 for e in results if e["ok"])
    print(f"完成：本轮新入库 {ok} 部，失败 {len(failures)} 部；库内现状：")
    print_coverage(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
