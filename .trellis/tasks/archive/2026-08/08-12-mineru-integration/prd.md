# MinerU 云 API 集成：PDF 财报转 Markdown

## Goal

在 dayu-agent 中集成 MinerU 云端精准解析 API，将本地下载的财报 PDF 转换为 Markdown 格式，替代 Docling 作为 PDF→Markdown 的转换引擎。暂不部署本地 MinerU，只通过官网 API 集成。

## What I already know

- PRD `docs/PRD_财报智能处理平台.md` v5.5 提供整体方案：新建 `mineru_export.py`（~120 行）+ `conversion_engine.py`（~40 行），后续 P2 再泛化 pipeline
- 现有 Docling 收敛点：`dayu/fins/docling_export.py`，类型别名 `PdfToDoclingJsonBytes = Callable[[bytes, str], bytes]`
- 下载链路注入点：`cn_download_protocols.py:188` `convert_pdf_to_docling_json` property，`cn_pipeline.py:174` 默认注入 `convert_pdf_bytes_to_docling_json_bytes`
- 产出物 `_docling.json`，`cn_download_source_upsert.py:238` 硬校验 `primary_document.endswith("_docling.json")`
- Processor 按后缀路由：`.md` 产出物自动命中 `FinsMarkdownProcessor`（priority=100），零改动
- API key 模式：env var 常量集中在 `dayu/contracts/env_keys.py`（如 `FMP_API_KEY_ENV = "FMP_API_KEY"`），读取用 `os.environ.get`
- run.json 支持 `{{ENV_VAR}}` 占位符替换（`config_loader.py:_replace_env_vars`）
- CLI 结构：`dayu/cli/arg_parsing.py` + `command_names.py` + `main.py` + `commands/fins.py`
- 无官方云端 Python SDK；MinerU 是本地 CLI/模型库，需 httpx 自封装 v4 接口

## Assumptions (temporary)

- 转换入口复用现有 `Callable[[bytes, str], bytes]` 签名风格，便于后续 P2 pipeline 泛化
- API key 走 env var（`MINERU_API_KEY` 或 `MINERU_TOKEN`），与 FMP_API_KEY 模式一致
- 使用精准解析 API v4（本地文件上传：`POST /api/v4/file-urls/batch` → PUT 上传 → 轮询 `GET /api/v4/extract-results/batch/{batch_id}` → 下载 zip 取 full.md）

## Open Questions

- ~~MVP 范围~~ → 已确认：**只做独立转换能力，不碰下载链路**（下载与转换解耦）
- ~~转换入口形态~~ → 已确认：**方案 A（CLI 命令 + 模块 API）**
- ~~API key 配置方式~~ → 已确认：**方案 1（env var）**
- ~~扩展范围~~ → 已确认：**最小方案（单文件转换）**，转换参数使用默认值，不做批量/CLI 可配参数

## Requirements (evolving)

- 新建 `dayu/fins/mineru_export.py`：封装 MinerU 云 API（申请上传 URL → PUT 上传 → 轮询 → 下载 zip → 取 full.md）
- 提供 `convert_pdf_bytes_to_markdown_bytes(raw_data, stream_name) -> bytes` 签名（对齐 Docling 收敛点）
- 新建 CLI 子命令 `dayu-cli convert`：`--pdf <path>` / `--output <path>`，读取本地 PDF → 调用 MinerU → 写 Markdown 落盘
- 配置管理：API key 从 `MINERU_API_KEY` env var 读取（`dayu/contracts/env_keys.py` 新增常量）
- 错误处理：网络错误、API 错误码（A0202/A0211/-60018 等）、轮询超时、zip 路径穿越防护
- 转换参数使用常量默认值（`model_version="vlm"`、`language="ch"`、`enable_formula=True`、`enable_table=True`、`poll_interval`、`poll_timeout`），不做 CLI 可配
- 测试：HTTP 调用序列（mock）、zip 解压取 md、错误分支

## 确认的决策（用户）

- **下载与转换解耦**：本任务只实现独立 PDF→Markdown 转换能力（MinerU API 客户端），**不修改下载链路**，不做 pipeline 泛化。下载链路产出 `_docling.json` 保持不变。
- **方案 A（CLI 命令 + 模块 API）**：
  - 新建 `dayu/fins/mineru_export.py`（核心 API 客户端，`convert_pdf_bytes_to_markdown_bytes`）
  - 新增 CLI 子命令（如 `dayu-cli convert --pdf <path> --output <out.md>`），读取本地 PDF → 调用 MinerU → 写 Markdown 落盘
  - 用户可直接用命令行转换本地已下载的 PDF
- **API key 走 env var（方案 1）**：新增 `MINERU_API_KEY`（或 `MINERU_TOKEN`）环境变量，与 `dayu/contracts/env_keys.py` 现有 `FMP_API_KEY_ENV` 约定一致；缺失时报明确错误。不落盘密钥，不做 run.json 间接配置。
- **最小方案（单文件转换）**：MVP 只做单个 PDF 的转换（CLI 一次一个 `--pdf`），转换参数（`model_version`/`language`/`enable_formula`/`enable_table`/轮询间隔/超时）在 `mineru_export.py` 内用常量默认值，不做 CLI 可配参数，不做批量。

## Acceptance Criteria (evolving)

- [ ] `convert_pdf_bytes_to_markdown_bytes` 能从本地 PDF bytes 返回 Markdown bytes
- [ ] `dayu-cli convert --pdf <path> --output <out.md>` 可将本地 PDF 转为 Markdown 落盘
- [ ] 转换参数使用默认值（不做 CLI 可配参数），CLI 仅 `--pdf` / `--output`
- [ ] API key 从 `MINERU_API_KEY` env var 读取，缺失时给出明确错误
- [ ] 轮询超时/失败给出明确错误，支持重试配置
- [ ] zip 解压有路径穿越防护
- [ ] 测试覆盖 mock HTTP 序列 + 错误分支

## Definition of Done (team quality bar)

- Tests added/updated (unit where appropriate)
- Lint / typecheck / CI green
- Docs/notes updated if behavior changes

## Out of Scope (explicit)

- 本地部署 MinerU
- 下载链路改造 / pipeline 泛化（`cn_download_*`、`docling_upload_service`、SEC pipeline 一律不动）
- Prefect 调度层
- 转换引擎抽象 `conversion_engine.py`（暂不引入，待 P2 需要时再加）
- 批量转换（一次多 PDF）、CLI 可配转换参数、定时/自动重试

## Technical Notes

- MinerU 官方文档：https://mineru.net/apiManage/docs（研究文件 `.trellis/tasks/08-12-mineru-integration/research/mineru-api.md`）
- 本地文件上传流程：`POST /api/v4/file-urls/batch`（≤50 文件）→ 对返回的 `file_url` 做 PUT（不设 Content-Type）→ 自动提交任务 → 轮询 batch 结果 → `full_zip_url` 下载 zip → 解压取 `full.md`
- 认证：`Authorization: Bearer <token>`，token 在 mineru.net API 管理页面创建
- 限流：单文件 ≤200MB/≤200 页，每天 1000 页高优额度，超出降级；国外 URL 会超时（-60008），本地文件无此问题
- 错误码：A0202 token 错误、A0211 token 过期、-60018 每日任务上限

## Technical Approach

### 文件清单

| 文件 | 动作 | 说明 |
|------|------|------|
| `dayu/contracts/env_keys.py` | 修改 | 新增 `MINERU_API_KEY_ENV = "MINERU_API_KEY"` |
| `dayu/fins/mineru_export.py` | 新建 | MinerU 云 API 客户端，核心转换函数 |
| `dayu/cli/command_names.py` | 修改 | 新增 `CONVERT_COMMAND = "convert"` |
| `dayu/cli/arg_parsing.py` | 修改 | 新增 `convert` subparser（`--pdf` / `--output`） |
| `dayu/cli/main.py` | 修改 | 新增 `convert` 命令分发分支 |
| `dayu/cli/commands/convert.py` | 新建 | `run_convert_command()` 实现 |
| `tests/fins/test_mineru_export.py` | 新建 | 客户端单元测试 |
| `tests/cli/test_convert_command.py` | 新建 | CLI 命令测试 |

### `mineru_export.py` 设计

```
公开 API:
  - convert_pdf_bytes_to_markdown_bytes(raw_data: bytes, stream_name: str) -> bytes
    将 PDF 字节流通过 MinerU 云 API 转为 Markdown 字节流

内部实现:
  - _request_upload_urls(api_key, stream_name) -> (batch_id, upload_url)
    POST /api/v4/file-urls/batch，返回 batch_id + 签名上传 URL
  - _upload_pdf(upload_url, pdf_bytes) -> None
    PUT 上传（不设 Content-Type）
  - _poll_batch_result(api_key, batch_id) -> zip_url
    轮询 GET /api/v4/extract-results/batch/{batch_id} 直到 done / failed / 超时
  - _download_and_extract_markdown(zip_url) -> str
    下载 zip → 解压（路径穿越防护）→ 读 full.md

配置常量（模块级）:
  - _MODEL_VERSION = "vlm"
  - _LANGUAGE = "ch"
  - _ENABLE_FORMULA = True
  - _ENABLE_TABLE = True
  - _POLL_INTERVAL_SECONDS = 5.0
  - _POLL_TIMEOUT_SECONDS = 600.0

错误类型（新建）:
  - MineruApiError（基类：认证失败 / API 错误码 / 网络错误）
  - MineruPollTimeoutError（轮询超时）
  - MineruResultError（zip 无 full.md / 解压异常 / 路径穿越）

HTTP 客户端：httpx（同步），已是项目依赖（pyproject.toml `httpx>=0.28.0`），与 downloaders 一致
```

### CLI 命令

```
dayu-cli convert --pdf <path> --output <out.md>
```

- `--pdf` 必选，本地 PDF 路径（校验存在）
- `--output` 必选，Markdown 输出路径（父目录不存在时自动创建）
- 读取 PDF bytes → 调用 `convert_pdf_bytes_to_markdown_bytes` → 写文件

## Decision (ADR-lite)

**Context**: dayu-agent 需要将本地财报 PDF 转 Markdown 以提升中文表格还原质量，替换 Docling；暂不本地部署 MinerU，只走官方云 API。
**Decision**:
- 只做独立转换能力，下载与转换解耦（用户明确）
- 方案 A：CLI 命令 + 模块 API（用户确认）
- API key 走 `MINERU_API_KEY` env var（与 FMP_API_KEY 约定一致）
- 最小 MVP：单文件转换，转换参数用常量默认值
**Consequences**:
- 下载链路仍产出 `_docling.json`，本任务不改变现状
- 后续 P2 泛化 pipeline 时复用 `convert_pdf_bytes_to_markdown_bytes` 签名（对齐 `Callable[[bytes, str], bytes]` 协议）
- 不做批量/CLI 可配参数，二期按需扩展
