# 单 flow 实现「下载 → Markdown 解析 → 原始指标提取」可行性分析

> 关联 PRD：`docs/PRD_财报智能处理平台.md`（v5.5）
> 分析对象：Prefect → Agent 链路中，在一个 Prefect flow 内同时完成财报 PDF 下载、Markdown 解析、原始指标提取
> 日期：2026-08-12

## 0. 结论

**可行，且比 PRD 现有设计（download → process → extract 三流程串联）更简洁。** 关键依据：

1. **download 已内嵌 PDF→Markdown 转换**——`run_cn_download_single_filing_stream`（`dayu/fins/pipelines/cn_download_filing_workflow.py`）在下载阶段就通过注入的 `convert_pdf_to_*` 函数产出 primary document。MinerU 替换后产出 `.md`，「PDF 下载 + Markdown 解析」天然是一个 pipeline 操作，单 flow 中不需要单独的 parse 阶段。
2. **Markdown 解析是惰性的**——`FinsToolService._create_processor`（`dayu/fins/tools/service.py:1761`）从 `get_primary_source()` 指向的 `.md` 创建 processor，解析发生在提取 agent 调用工具时。`get_financial_statement` / `read_section` 等工具**不需要**先跑 `process` 命令就能工作。
3. **AsyncAgent 可脱离 Host 独立构建**——构造函数只需 `runner` + `tool_executor`（`dayu/engine/async_agent.py:445`），`run_and_wait()` 提供非流式结果，Prefect task 直接驱动可行。
4. **download 结果携带 document_id**——`DownloadResultData.filings[].document_id`（`service_runtime.py:849`），提取 target 构造零成本。

> **重要澄清**：PRD 中「process（预处理）」阶段对**指标提取**非必需。`process` 生成的是 `processed/` 快照，服务于 CI 评分、`read_section` 的预渲染加速等，不是 `get_financial_statement` 的前置条件。单 flow 若只做提取，可安全跳过 process，缩短链路、减少一次全量解析。

---

## 1. 三段链路在现有代码中的真实形态

### 1.1 下载（已含转换）

```
FinsRuntime.execute(DOWNLOAD)                    // service_runtime.py:1737
  → pipeline.download_stream()                    // cn_pipeline.py:323
    → FinsIngestionService.download_stream()      // ingestion/service.py:74
      → backend.download_stream()                 // cn_download_workflow.py
        → run_cn_download_single_filing_stream()  // cn_download_filing_workflow.py:83
            ├─ PDF 下载（pdf_download_gate 限流）
            └─ convert_pdf_to_docling_json(...)   // 注入点，MinerU 替换后产出 .md
            → commit_cn_filing_source_document()  // primary_document 指向产出物
```

- 转换注入点是 `cn_download_filing_workflow.py:90` 的 `convert_pdf_to_docling_json: Callable[[bytes, str], bytes]`，PRD §3 已规划改为 `convert_pdf_to_markdown_bytes`。
- 同步聚合入口：`pipeline.download()` → `_run_async_ingestion_sync(...)`，返回含 `filings[]` 的 result dict（`service_runtime.py:877`）。

### 1.2 Markdown 解析（惰性，工具调用时触发）

```
FinsToolService.get_financial_statement()         // tools/service.py:1018
  → _get_or_create_processor()                    // tools/service.py:~1740
    → _create_processor()                         // tools/service.py:1761
      → source_repository.get_primary_source()    // 指向 .md
      → processor_registry.create_with_fallback() // .md → FinsMarkdownProcessor
```

- processor 解析发生在工具调用时刻，与 `process` 命令无关。
- 结论：**「Markdown 解析」不是单 flow 中的一个显式阶段，而是提取 agent 工具循环的隐式能力**。单 flow 实际只有两个显式阶段：download + extract。

### 1.3 指标提取（agent 工具循环）

- `AsyncAgent(runner, tool_executor)` 独立构建（`async_agent.py:445`），`run_and_wait(prompt)` 非流式返回（`async_agent.py`）。
- ToolRegistry 需注入 `fins_read`（`get_financial_statement` 等）+ `fins_write`（`upsert_financial_metric` 等）工具。
- 数据落 DuckDB：`MetricStore.upsert_metric()`（PRD §4.7.7，新建）。

---

## 2. 单 flow 编排结构

```python
@flow(name="ingest_and_extract_flow")
def ingest_and_extract_flow(tickers, *, forms=None, start=None, end=None, workspace_root=...):
    # 阶段 1：批量下载（内嵌 PDF→.md 转换）
    download_results = {
        t: download_single_ticker_task(t, ...) for t in tickers
    }
    # 阶段 2：从成功 filings 构造提取 target
    targets = build_extract_targets(download_results)
    # 阶段 3：按文档粒度提取
    extract_results = {
        k: extract_single_document_task(target, ctx) for k, target in targets.items()
    }
    return {"download": download_results, "extract": extract_results}

@task(retries=3, retry_delay_seconds=[60, 120, 300])
def download_single_ticker_task(ticker, ctx, *, forms, start, end, overwrite) -> DownloadResult:
    # 直接消费 FinsRuntime.execute(DOWNLOAD) 或 pipeline.download()
    ...

@task(retries=2, retry_delay_seconds=60)
def extract_single_document_task(target, ctx) -> ExtractResult:
    # build_fins_tool_service + build_extract_tool_registry
    # resolve_extract_prompt_path → 渲染 prompt
    # AsyncAgent(runner, tool_executor).run_and_wait(prompt)
    # MetricStore 读取提取结果 → ExtractResult
    ...
```

### 2.1 数据交接

| 交接点 | 来源 | 消费方 |
|--------|------|--------|
| `download_results[t].filings[].document_id` | `service_runtime.py:852` | `build_extract_targets()` |
| `filings[].status` | 同上 | 过滤成功文档（`FILING_COMPLETED`） |
| `filings[].form_type` / `report_date` | 同上 | 提取 target（`statement_type` 由 CLI 或默认三表） |
| CompanyMeta（market/industry/currency/company_name） | 提取时读 `meta.json` | prompt 变量注入（PRD §4.9.4） |
| source meta.json（fiscal_year/period） | 提取时读 | prompt 变量注入 |

- `DownloadFilingResultItem` 不含 fiscal_year/fiscal_period，但 `ExtractTaskRunner` 运行时从 source meta.json 读取即可，target 只需 `(ticker, document_id, statement_type)`。

### 2.2 单 flow 下是否还需要 process？

| 场景 | 是否跑 process | 说明 |
|------|---------------|------|
| 只提取 `get_financial_statement` 三张表 | **否** | 工具从 `.md` 惰性解析，无需 process |
| 提取需要 `read_section` / `get_table`（叙述段+表格） | 否（功能上） | processor 同样从 `.md` 创建；process 快照只是加速/CI 用 |
| 需要 CI 评分 / `processed/` 快照 / 跨文档一致性 | 是 | 非提取必需，属独立目标 |

> **PRD 修订建议**：PRD §6.5 `full_pipeline_flow` 的 download → process → extract 顺序，在「仅提取指标」语义下可省略 process；如需保留，应在 flow 中注明 process 是为 CI/快照服务而非提取前置。

---

## 3. 关键设计点与风险

### 3.1 asyncio 事件循环约束（必须处理）

`_run_async_ingestion_sync`（`ingestion/service.py:263`）在**已存在运行中事件循环**时会抛 `RuntimeError`。Prefect 2.x 支持 async flow/task，两种写法：

| 写法 | download 消费方式 | 说明 |
|------|------------------|------|
| **async flow + async task**（推荐） | `async for event in pipeline.download_stream(...)` 直接消费 | 不调用同步 `download()`，避免 `asyncio.run` 冲突；提取 agent 的 `run_and_wait()` 也是 awaitable，天然契合 |
| **sync flow + sync task** | `pipeline.download()`（内部 `asyncio.run`） | 简单，但 flow 内无法 await agent，提取需再包一层 `asyncio.run(agent.run_and_wait())` |

推荐 async flow：download 用 `download_stream` 迭代收集，提取用 `await agent.run_and_wait(prompt)`，全程不依赖 `asyncio.run`。

### 3.2 失败隔离

- download 按 ticker 粒度 task，某 ticker 下载失败不影响其他 ticker。
- `build_extract_targets` 只收 `status == FILING_COMPLETED` 且 `downloaded_files > 0` 的 filings。
- extract 按 document 粒度 task，单个文档提取失败（LLM 超时/工具错误）重试或标记，不影响同 ticker 其他文档。
- **不建议**把 download+extract 包成同一个 ticker 级 task——那样失败重试会整体重跑，浪费 MinerU 转换/下载。

### 3.3 并发控制（沿用 PRD §5.5.1）

- download：pipeline 内部 `pdf_download_gate`（cn_download lane）仍然生效，Prefect task concurrency 作为外部兜底。
- extract：**必须**用 `extract_single_document_task.with_options(concurrency_limit=N)` 限制 LLM 并发，因为绕过 Host 后 `llm_api` lane 不生效。
- MinerU API 等待发生在 download 阶段（阻塞型 HTTP 轮询），注意该 task 的 retry 策略区分 API 超时（`mineru_api_timeout`）与网络错误。

### 3.4 单 flow vs 多 flow 权衡

| 维度 | 单 flow（ingest_and_extract） | 多 flow（PRD 现状） |
|------|------------------------------|---------------------|
| 端到端编排 | 一次调用完成 | 手动串联或包一层 full_pipeline |
| 参数传递 | 内部交接，无跨 flow 序列化 | 每个 flow 独立入参 |
| 重跑粒度 | 粗（全链路），靠 task 缓存缓解 | 细（可按阶段重跑） |
| 中间态观察 | 只有 Prefect task 状态 | 每个 flow 一个状态页 |
| task 缓存 | download 缓存 key 24h / extract 永久（PRD §5.5.4） | 相同 |

**建议**：保留单 flow 作为入口（`dayu-cli flow ingest-and-extract`），内部仍用独立 task，Prefect 按 task 粒度缓存/重试。若未来需要「只下载不提取」或「只提取已下载」，再加薄壳 flow 复用同一批 task。

### 3.5 单 flow 对 PRD 其他模块的依赖

| 依赖 | 现状 | 阻塞 |
|------|------|------|
| MinerU 转换（产出 `.md`） | P1 新建，未实现 | 是（否则提取读不到 `.md`） |
| `cn_download_source_upsert.py` 后缀校验放宽 | PRD §3.4，未改 | 是 |
| `FinsMarkdownProcessor` 路由 `.md` | 已存在，零改动 | 否 |
| `FinsToolService` 写工具 + MetricStore | P4，未实现 | 是 |
| 提取 prompt 分层解析 + ExtractionRunner | P5/P9，未实现 | 是 |
| `build_fins_tool_service`（脱离 Host 构建） | 需新建辅助函数 | 是（`FinsRuntime.get_tool_service()` 可复用，见 §4） |

---

## 4. 具体实施建议（对 PRD 的增量）

1. **PRD §5.4.5 / §6.5**：新增 `dayu/flows/ingest_extract_flow.py`（~120 行），`build_extract_targets(download_results)` 作为独立 helper（~40 行），供单 flow 与多 flow 复用。
2. **PRD §5.4.2**：`FlowRuntimeContext` 增加 `tool_service`（复用 `FinsRuntime.get_tool_service()`，`service_runtime.py:137`）与 `metric_store`，让 extract task 直接拿到读写工具链，避免每次重建 FinsToolService。
3. **明确 process 定位**：单 flow 默认跳过 process；需要 CI/快照时通过 `run_process: bool = False` 参数开启。PRD §8.7 端到端验证的「download → process → extract」可改为「download → extract」，process 仅在有 CI 需求时加入。
4. **async flow 约定**：在 PRD §5.5.2 补充「单 flow 采用 async flow + async task，download 消费 stream、提取 await agent」，明确不使用 `asyncio.run`。

---

## 5. 验证清单（增量）

| 验证项 | 方法 |
|--------|------|
| 单 flow 全链路 | `dayu-cli flow ingest-and-extract --tickers 600519,09988`，确认两 ticker 下载 `.md` 并提取指标到 DuckDB |
| 跳过 process | 确认该 flow 不产生 `processed/` 快照，且 `get_financial_statement` 正常返回 |
| 下载失败隔离 | 一个 ticker 下载失败，另一个 ticker 仍完成下载+提取 |
| LLM 并发限制 | 单 flow 内 5 文档并行提取时，Prefect concurrency limit 生效 |
| 缓存命中 | 重跑单 flow，download task（24h）与 extract task（永久）命中缓存不重执行 |
| async 循环无冲突 | 确认 flow 全程无 `RuntimeError: 检测到正在运行的事件循环` |
