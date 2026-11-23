# cn-law-verify

**给 AI 的中国法条核验层：查得到、验得真、带效力与出处。**

一个面向多 Agent 通用的 MCP server ＋配套 Skill。大模型引用中国法律法规
时最危险的错误不是文风，而是**编造法条、引错条号、拿已废止的旧条文当
现行法**。cn-law-verify 把核验从「靠模型自觉」变成「靠代码执行」：
引文写入输出之前，先向国家法律法规数据库官方源逐条核验——
存在性、原文逐字、效力状态，三检缺一不可。

> An MCP server + Skill that verifies Chinese legal citations against the
> official National Laws and Regulations Database: existence, verbatim
> text, and validity status — checked by code, not by the model's memory.

## 快速开始

零依赖，只需要 Python 3.10+：

```bash
git clone https://github.com/songoao25/cn-law-verify.git
cd cn-law-verify
python3 server/cn_law_verify_server.py --version
```

在 MCP 客户端中注册（stdio）。仓库根目录已附 `.mcp.json` 示例：

```json
{
  "mcpServers": {
    "cn-law-verify": {
      "command": "python3",
      "args": ["server/cn_law_verify_server.py"]
    }
  }
}
```

配套规矩层见 [SKILL.md](SKILL.md)：先查后引、引用必带效力状态与官方
链接、查不到就明说、发现废止必须点名排除。

## 工具

| 工具 | 说明 | 状态 |
|---|---|---|
| `flk_search` | 在国家法律法规数据库官方源实时检索法规，返回效力状态与官方链接 | ✅ 可用 |
| `verify_citation` | 核验一条引注：法名＋条号（＋可选引文）→ 存在性／效力／原文逐字三检结论 | ✅ 可用 |
| `get_law` | 单部法规元数据＋目录＋版本沿革 | 开发中（P1） |
| `get_article` | 单条条文逐字原文（快照库通道） | 开发中（P1） |
| `search_articles` | 跨法规条文级全文检索（快照库通道） | 开发中（P1） |
| `check_citations_in_text` | 整段文本抽取全部引注逐条核验 | 开发中（P2，抽取模块已就绪） |
| `lookup` | 一句话直达：机器结论／人工复核清单两段式输出 | 开发中（P2） |
| `check_support` | 蕴含核验：引注是否真的支持论断 | 规划中（v2） |

全部工具只读（`readOnlyHint: true`），每次返回都带数据来源与取数时间。

### verify_citation 示例

入参：`{"law": "中华人民共和国民法典", "article": "第五百七十七条",
"citation_text": "当事人一方不履行合同义务或者履行合同义务不符合约定的，
应当承担继续履行、采取补救措施或者赔偿损失等违约责任。"}`

返回要点：法规定位（官方 `bbbs` 与详情页链接）、效力状态「有效」、
官方原文整条截取、逐字比对 `exact_match: true`、总判定
`verdict: "verified"`。法名不存在、条号漂移、法规已废止、引文改字，
都会被分别指出并给出 `not_verified`——而不是含糊地「仅供参考」。

核验结论恒为三态：**已核验**（verified）／**仅确认存在**
（exists_only，效力或比对有保留并写明）／**未通过核验**
（not_verified）。

## 它和同类项目的关系

- 「查得到原文」的检索型项目已经不少；本项目的差异在**核验**：
  第一个把「引文抽取 → 逐条回官方源比对 → 三态结论」做成确定性
  代码管线，并配公开测试与评测集（P2）。
- 数据策略是**快照库＋实时复核双通道**（P1 起）：本地快照保证
  稳定可复现，实时通道复核最新效力；实时不可用时明确标注结论
  依据的快照截止日，绝不静默用旧数据冒充。
- 设计细节（经验继承表、缺点解法表、数据源与版本策略）见
  [docs/DESIGN.md](docs/DESIGN.md)。

## 路线图

| 阶段 | 内容 |
|---|---|
| P0 | 骨架＋引文抽取/条号归一化＋`flk_search`、`verify_citation` 实时通道打通＋基础测试（**当前**） |
| P1 | 快照库：flk 公开文件离线建条级 SQLite 库、四态版本模型、增量包分发 |
| P2 | SKILL.md 定稿、200 题中文核验评测集、Streamable HTTP 传输、司法部行政法规库与国家规章库通道补齐 |
| P3 | 产品接入：念念助手经远程 MCP 接入同一核验内核，法律类回复发出前强制核验 |
| P4 | 蕴含核验、法规变迁（citator）增强、判例侧授权接入评估 |

## 安全与数据口径

- 数据只来自官方源：请求只发往白名单官方域名、全程 HTTPS 并开启
  TLS 证书校验，下载跳转逐跳复检域名；遇官方验证页立即终止。
- server 无写工具、不留存查询内容；本地 stdio 模式下，核验只把
  法名与条号发往官方源，待核验文本不出本机。**托管模式下**文本会
  到达 server 运营方，请知情选择部署方式。
- 法规文本源自国家法律法规数据库（flk.npc.gov.cn）公开数据；
  该库无公开 API，本项目使用其公开前端接口，并遵守「不批量抓取」
  的边界（快照数据经官方下载通道＋人工确认更新）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

含离线单测与联网集成用例（真法条命中／编造法名／条号漂移／
文本错位／已废止旧条文五类）。联网用例依赖官方源可用性，
可用 `CN_LAW_VERIFY_LIVE=0` 跳过。

## 致谢

- **flk-mcp**（[@lh4718655-jpg](https://github.com/lh4718655-jpg) 分享的开源项目）：本项目受其启发——三库分工、
  效力标注、链接溯源、白名单纪律等经验均继承自它，核验拦截、
  TLS 纪律与测试体系为本项目补齐的部分。独立重写实现，未复制其代码。
- [tlgzs/china-law-search](https://github.com/tlgzs/china-law-search)
  （壹律法条）：flk 全量快照库与数据流水线纪律的先行者。
- [thu-lawyer/china-law-mcp](https://github.com/thu-lawyer/china-law-mcp)：
  `verify_citation`／`check_citations_in_text` 工具形态的先行验证者，
  本项目与之同名同义，遵循生态惯例。

## License

代码：[MIT](LICENSE)。法规文本为国家法律法规数据库公开数据，
权利归属从其官方口径。
