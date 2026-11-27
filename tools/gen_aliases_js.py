#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 server/cn_law_verify_server.py 的 _LAW_ALIASES 生成 cloud/aliases.js。

cloud/aliases.js 头部注明“程序化生成，勿手改”——以此脚本为准，
简称表改动后跑一次即可同步 Worker 端（含 867 增补曾漏同步的教训）。
用法：python3 tools/gen_aliases_js.py
"""
import ast
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    src = os.path.join(ROOT, "server", "cn_law_verify_server.py")
    tree = ast.parse(open(src, encoding="utf-8").read())
    aliases = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_LAW_ALIASES"
            for t in node.targets
        ):
            aliases = ast.literal_eval(node.value)
    assert aliases, "_LAW_ALIASES not found"
    out = os.path.join(ROOT, "cloud", "aliases.js")
    body = json.dumps(aliases, ensure_ascii=False, sort_keys=True)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(
            "// 程序化生成自 server/cn_law_verify_server.py 的 _LAW_ALIASES，勿手改。\n"
            "// 生成：python3 tools/gen_aliases_js.py\n"
            f"export const LAW_ALIASES = {body};\n"
        )
    print(f"{len(aliases)} aliases -> {out}")


if __name__ == "__main__":
    main()
