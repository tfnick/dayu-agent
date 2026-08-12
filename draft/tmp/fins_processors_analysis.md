# Fins Processors 架构分析

## 1. 总体定位

`dayu/fins/processors/` 是 Fins 领域的文档处理器子包。它的核心职责是：**把已入库的财报文档（Docling JSON / HTML / Markdown）解析为 LLM 工具可消费的结构化数据**——章节、表格、财务报表、XBRL 数据。

它不是架构分层，而是 Fins 领域包的内部组件，被 `FinsToolService`（工具层）和 pipeline（下载/上传链路）共同消费。

## 2. 文件清单与职责

### 2.1 包入口与注册表

| 文件 | 主要导出 | 职责 |
|------|---------|------|
| `__init__.py` | 全部处理器类 + 两个 registry 构建函数 | 包入口，统一导出 |
| `registry.py` | `build_fins_processor_registry()`, `build_bs_experiment_registry()` | 在 engine 注册表基础上追加 fins 特化处理器，组装优先级链 |

### 2.2 通用增强处理器（覆盖 engine 三大处理器）

| 文件 | 主要类 | 职责 |
|------|--------|------|
| `fins_docling_processor.py` | `FinsDoclingProcessor` | 继承 engine `DoclingProcessor`，解析后对表格执行 `relabel_tables` 金融语义标注 |
| `fins_bs_processor.py` | `FinsBSProcessor` | 继承 engine `BSProcessor`，补充金融标注 + SEC layout 表格检测 + EDGAR SGML 信封剥离 |
| `fins_markdown_processor.py` | `FinsMarkdownProcessor` | 继承 engine `MarkdownProcessor`，补充 `relabel_tables` 金融语义标注 |

### 2.3 金融数据协议与语义增强

| 文件 | 主要类/函数 | 职责 |
|------|------------|------|
| `financial_base.py` | `FinancialDataProcessor`(Protocol), `FinancialStatementResult`, `XbrlFactsResult`, `FinancialMeta` | 定义金融数据能力协议和 TypedDict，仅 fins 层使用 |
| `financial_enhancer.py` | `FinsProcessorMixin`, `relabel_tables()`, `is_financial_table()`, `extra_financial_table_fields()` | 表格金融语义增强：统一关键词库、判定规则、重标注流程 |

### 2.4 SEC 通用处理器与工具子模块

| 文件 | 主要类/函数 | 职责 |
|------|------------|------|
| `sec_processor.py` | `SecProcessor` | 基于 edgartools 的 SEC 通用处理器，提供章节/表格/搜索/XBRL 能力，是 edgartools 路线处理器的基座 |
| `form_type_utils.py` | `normalize_form_type()` | SEC 表单类型标准化工具，所有处理器共享 |
| `sec_html_rules.py` | `strip_edgar_sgml_envelope()`, `is_sec_layout_table()`, `is_sec_cover_page_table()` | SEC/EDGAR HTML 规则：layout 表格检测、SGML 信封剥离、封面页识别 |
| `sec_dom_helpers.py` | `_extract_text_from_raw_html()`, `_extract_dom_table_contexts()` | SEC 文档 DOM/HTML 解析工具：提取纯文本和表格前文上下文 |
| `sec_section_build.py` | `_SectionBlock`, `_build_sections()`, `_safe_document_text()` | SEC 文档章节切分与定位 |
| `sec_table_extraction.py` | `_build_tables()`, `_render_markdown_table()` 等 | SEC 表格提取、渲染、分类、章节匹配 |
| `sec_xbrl_query.py` | `_STATEMENT_METHODS`, `_query_facts_rows()`, `build_statement_locator()` 等 | XBRL 查询与财务报表结构化提取 |

### 2.5 虚拟章节公共 mixin

| 文件 | 主要类/函数 | 职责 |
|------|------------|------|
| `sec_form_section_common.py` | `_VirtualSectionProcessorMixin`, `_VirtualSection`, `_dedupe_markers()` 等 | SEC 表单专项章节处理器公共能力：基于全文 marker 的虚拟章节切分 mixin，被全部 14 个表单处理器共享 |

### 2.6 报告类表单基类（双轨）

| 文件 | 主要类 | 职责 |
|------|--------|------|
| `sec_report_form_common.py` | `_BaseSecReportFormProcessor` | edgartools 路线报告类基类，继承 `SecProcessor`，提供 TOC 去噪、Item marker 选取、XBRL + HTML fallback |
| `bs_report_form_common.py` | `_BaseBsReportFormProcessor` | BS 路线报告类基类，继承 `FinsBSProcessor`，独立加载 XBRL，不依赖 edgartools |
| `html_financial_statement_common.py` | `build_html_statement_result_from_tables()` 等 | HTML 财务报表结构化共享核心，与表单类型无关 |
| `report_form_financial_statement_common.py` | `REPORT_FORM_SUPPORTED_STATEMENT_TYPES`, `select_report_statement_tables()` | 报告类表单财务表语义层：报表类型分类规则、候选表筛选 |

### 2.7 表单公共常量模块（marker 共享层）

| 文件 | 主要导出 | 职责 |
|------|---------|------|
| `ten_k_form_common.py` | `_TEN_K_ITEM_ORDER`, `_build_ten_k_markers()`, `expand_ten_k_virtual_sections_content()` | 10-K 共享常量与 marker 构建 |
| `ten_q_form_common.py` | `_TEN_Q_ITEM_PATTERN`, `_build_ten_q_markers()`, `expand_ten_q_virtual_sections_content()` | 10-Q 共享常量与 marker 构建，含 Part I/II 两阶段 Item 选取 |
| `twenty_f_form_common.py` | `_TWENTY_F_ITEM_ORDER`, `_build_twenty_f_markers()` | 20-F 共享常量与 marker 构建 |
| `eight_k_form_common.py` | `_EIGHT_K_ITEM_PATTERN`, `_build_eight_k_markers()` | 8-K 共享常量与 marker 构建 |
| `six_k_form_common.py` | `_build_six_k_markers()`, `_classify_statement_type_for_table()` | 6-K 共享常量、marker 与报表分类逻辑 |
| `def14a_form_common.py` | `_DEF14A_SECTION_MARKERS`, `_build_def14a_markers()` | DEF 14A 共享常量与 marker 构建 |
| `sc13_form_common.py` | `_SC13_ITEM_PATTERN`, `_build_sc13_markers()` | SC 13D/G 共享常量与 marker 构建 |

### 2.8 BS 路线表单处理器（priority 200，主路径）

| 文件 | 主要类 | 职责 |
|------|--------|------|
| `bs_ten_k_processor.py` | `BsTenKFormProcessor` | BS 路线 10-K 处理器，继承 `_BaseBsReportFormProcessor` |
| `bs_ten_q_processor.py` | `BsTenQFormProcessor` | BS 路线 10-Q 处理器，继承 `_BaseBsReportFormProcessor` |
| `bs_twenty_f_processor.py` | `BsTwentyFFormProcessor` | BS 路线 20-F 处理器，继承 `_BaseBsReportFormProcessor` |
| `bs_eight_k_processor.py` | `BsEightKFormProcessor` | BS 路线 8-K 处理器，继承 `FinsBSProcessor`，无 XBRL |
| `bs_sc13_processor.py` | `BsSc13FormProcessor` | BS 路线 SC 13 处理器，继承 `FinsBSProcessor`，无 XBRL |
| `bs_six_k_processor.py` | `BsSixKFormProcessor` | BS 路线 6-K 处理器，继承 `FinsBSProcessor`，含独立 XBRL 加载 |
| `bs_def14a_processor.py` | `BsDef14AFormProcessor` | BS 路线 DEF 14A 处理器，继承 `FinsBSProcessor` |

### 2.9 edgartools 路线表单处理器（priority 190，回退路径）

| 文件 | 主要类 | 职责 |
|------|--------|------|
| `ten_k_processor.py` | `TenKFormProcessor` | edgartools 路线 10-K 处理器，继承 `_BaseSecReportFormProcessor` |
| `ten_q_processor.py` | `TenQFormProcessor` | edgartools 路线 10-Q 处理器，继承 `_BaseSecReportFormProcessor` |
| `twenty_f_processor.py` | `TwentyFFormProcessor` | edgartools 路线 20-F 处理器，继承 `_BaseSecReportFormProcessor` |
| `eight_k_processor.py` | `EightKFormProcessor` | edgartools 路线 8-K 处理器，继承 `_BaseSecReportFormProcessor` |
| `sc13_processor.py` | `Sc13FormProcessor` | edgartools 路线 SC 13 处理器，继承 `_BaseSecReportFormProcessor` |
| `def14a_processor.py` | `Def14AFormProcessor` | edgartools 路线 DEF 14A 处理器，继承 `_BaseSecReportFormProcessor` |

> 注：6-K 没有 edgartools 路线回退处理器，BS 路线 `BsSixKFormProcessor` 是唯一实现。

## 3. Processor 继承体系

```
Engine 层 (dayu/engine/processors/)
├── MarkdownProcessor
├── DoclingProcessor
├── BSProcessor
└── (Protocol) DocumentProcessor

Fins 层 (dayu/fins/processors/)
├── FinsProcessorMixin (mixin, financial_enhancer.py)
│   ├── FinsMarkdownProcessor(FinsProcessorMixin, MarkdownProcessor)
│   ├── FinsDoclingProcessor(FinsProcessorMixin, DoclingProcessor)
│   └── FinsBSProcessor(FinsProcessorMixin, BSProcessor)
│       │
│       ├── _VirtualSectionProcessorMixin (mixin, sec_form_section_common.py)
│       │   ├── _BaseBsReportFormProcessor(_VirtualSectionProcessorMixin, FinsBSProcessor)
│       │   │   ├── BsTenKFormProcessor
│       │   │   ├── BsTenQFormProcessor
│       │   │   └── BsTwentyFFormProcessor
│       │   ├── BsEightKFormProcessor(_VirtualSectionProcessorMixin, FinsBSProcessor)
│       │   ├── BsSc13FormProcessor(_VirtualSectionProcessorMixin, FinsBSProcessor)
│       │   ├── BsSixKFormProcessor(_VirtualSectionProcessorMixin, FinsBSProcessor)
│       │   └── BsDef14AFormProcessor(_VirtualSectionProcessorMixin, FinsBSProcessor)
│       │
├── SecProcessor (独立类，不继承 BSProcessor)
│   └── _VirtualSectionProcessorMixin (mixin)
│       └── _BaseSecReportFormProcessor(_VirtualSectionProcessorMixin, SecProcessor)
│           ├── TenKFormProcessor
│           ├── TenQFormProcessor
│           ├── TwentyFFormProcessor
│           ├── Def14AFormProcessor
│           ├── EightKFormProcessor
│           └── Sc13FormProcessor
```

关键设计：
- BS 路线报告类处理器（10-K/10-Q/20-F）统一继承 `_BaseBsReportFormProcessor`，获得 XBRL 延迟加载和虚拟章节切分。
- BS 路线非报告类处理器（8-K/SC13/6-K/DEF14A）直接继承 `FinsBSProcessor`，因为它们不需要 XBRL 或自行管理 XBRL。
- edgartools 路线全部表单处理器继承 `_BaseSecReportFormProcessor`。
- `_VirtualSectionProcessorMixin` 是双轨共享的虚拟章节切分 mixin。

## 4. Processor 注册表构建逻辑

`registry.py` 的 `build_fins_processor_registry()` 按四步组装优先级链：

```
步骤 1: build_engine_processor_registry()
  → 注册 DoclingProcessor, MarkdownProcessor, BSProcessor (priority=10)

步骤 2: 覆盖注册 fins 增强处理器
  → FinsDoclingProcessor  (name="docling_processor",  priority=100, overwrite=True)
  → FinsMarkdownProcessor (name="markdown_processor", priority=100, overwrite=True)
  → FinsBSProcessor       (name="bs_processor",      priority=80,  overwrite=True)

步骤 3: 注册 SEC 表单专项处理器（双轨：BS 主 + edgartools 回退）
  → BsSc13FormProcessor(200)    + Sc13FormProcessor(190)
  → BsSixKFormProcessor(200)                            ← 6-K 无 edgartools 回退
  → BsDef14AFormProcessor(200)  + Def14AFormProcessor(190)
  → BsEightKFormProcessor(200)  + EightKFormProcessor(190)
  → BsTenKFormProcessor(200)    + TenKFormProcessor(190)
  → BsTenQFormProcessor(200)    + TenQFormProcessor(190)
  → BsTwentyFFormProcessor(200) + TwentyFFormProcessor(190)

步骤 4: 通用兜底
  → SecProcessor (name="sec_processor", priority=120)
```

优先级常量：

| 常量 | 值 | 含义 |
|------|-----|------|
| `_SPECIAL_FORM_PRIORITY` | 200 | BS 路线表单处理器（主路径） |
| `_REPORT_FORM_FALLBACK_PRIORITY` | 190 | edgartools 路线表单处理器（回退） |
| `_SEC_PROCESSOR_PRIORITY` | 120 | SecProcessor 通用兜底 |
| `_FINS_DOC_MARKDOWN_PRIORITY` | 100 | fins 增强 Docling/Markdown |
| `_FINS_BS_PRIORITY` | 80 | fins 增强 BS |

注册表内部按 priority 降序排序，`resolve_candidates()` 遍历时自然按优先级从高到低返回。

## 5. Processor 分派机制

`ProcessorRegistry`（engine 层 `dayu/engine/processors/processor_registry.py`）的分派流程：

```
create_with_fallback(source, form_type, media_type)
  │
  ├─ resolve_candidates(source, form_type, media_type)
  │    └─ 遍历 _items（已按 priority 降序）
  │       对每个 registration.processor_cls 调用:
  │         cls.supports(source, form_type=..., media_type=...)
  │       返回全部 supports()=True 的类列表（保持优先级顺序）
  │
  ├─ 依次尝试实例化每个候选:
  │    processor_cls(source=source, form_type=form_type, media_type=media_type)
  │    成功 → 返回实例
  │    失败 → 记录错误，调用 on_fallback 回调，继续下一候选
  │
  └─ 全部失败 → raise RuntimeError
```

分派信号是 **source（文件路径/URI）+ form_type + media_type** 三元组：

- **表单处理器**：先 `normalize_form_type(form_type)`，检查是否在 `_SUPPORTED_FORMS` 中，再检查底层引擎是否能解析该文件类型。
- **通用处理器**（Docling/Markdown/BS）：基于文件后缀和 media_type 判定。
- **SecProcessor**：检查文件后缀是否为 `.htm/.html/.xhtml/.xml`，form_type 是否在 `_SUPPORTED_FORMS` 中。

### 分派示例

AAPL 10-K（`form_type="10-K"`, `media_type="text/html"`, 文件 `aapl-20240928.htm`）：

| 优先级 | 候选处理器 | supports() 判定 |
|--------|-----------|----------------|
| 200 | `BsTenKFormProcessor` | form_type=="10-K" ✓, HTML 可解析 ✓ → **命中** |
| 190 | `TenKFormProcessor` | 不再尝试（200 已成功） |
| 120 | `SecProcessor` | 不再尝试 |
| 100 | `FinsDoclingProcessor` | 不再尝试 |

A 股年报（`form_type="FY"`, `media_type="application/pdf"`, 文件 `fil_cn_xxx_docling.json`）：

| 优先级 | 候选处理器 | supports() 判定 |
|--------|-----------|----------------|
| 200 | `BsTenKFormProcessor` | form_type=="FY" 不在 _SUPPORTED_FORMS ✗ |
| ... | (所有 SEC 专项处理器均不匹配) | |
| 100 | `FinsDoclingProcessor` | 文件后缀 `_docling.json` ✓ → **命中** |

## 6. 完整调用链路

### 6.1 注册阶段（FinsRuntime 初始化时）

```
DefaultFinsRuntime.create(workspace_root)
  → build_fins_processor_registry()
    → build_engine_processor_registry()    ← engine 基座
    → 追加 fins 增强处理器                   ← 覆盖 engine
    → 追加 SEC 专项处理器                   ← 双轨
    → 追加 SecProcessor                    ← 通用兜底
  → ProcessorRegistry 实例存入 FinsRuntime
```

### 6.2 运行时工具调用

```
LLM tool call: search_document(ticker="AAPL", document_id="fil_xxx", query="risk factors")
  │
  ├─ FinsToolService.search_document(ticker, document_id, query)
  │    dayu/fins/tools/service.py
  │
  ├─ _get_or_create_processor(ticker, document_id)
  │    ├─ 检查 ProcessorLRUCache (key = ticker + document_id)
  │    │   命中 → 返回缓存实例
  │    │
  │    └─ _create_processor(ticker, document_id)
  │         ├─ source_repository.get_primary_source(ticker, doc_id, source_kind)
  │         │   → Source 对象（封装文件路径/URI）
  │         ├─ source_repository.get_source_meta(ticker, doc_id, source_kind)
  │         │   → 读取 meta.json 获取 form_type
  │         └─ processor_registry.create_with_fallback(
  │                source=source,
  │                form_type="10-K",
  │                media_type="text/html",
  │            )
  │            → resolve_candidates() → [BsTenKFormProcessor(200), TenKFormProcessor(190), ...]
  │            → 尝试 BsTenKFormProcessor(source, form_type="10-K", media_type="text/html")
  │            → 成功 → 返回实例
  │
  └─ processor.search(query="risk factors")
       → _VirtualSectionProcessorMixin 虚拟章节内搜索
       → 返回 SearchHit 列表
```

### 6.3 上传/下载链路中的 processor 调用

```
upload_filing / cn_download pipeline
  → DoclingUploadService / cn_download_filing_workflow
    → 不直接调用 ProcessorRegistry
    → 只负责把 PDF 转成 Docling JSON 并落盘
    → ProcessorRegistry 在后续工具调用时才被使用
```

## 7. BS 路线 vs edgartools 路线的双轨设计

| 维度 | BS 路线 | edgartools 路线 |
|------|---------|----------------|
| HTML 解析引擎 | BeautifulSoup（lxml parser） | edgartools `HTMLParser` + `ParserConfig` |
| 基座处理器 | `FinsBSProcessor → BSProcessor` | `SecProcessor`（独立类） |
| 报告类基类 | `_BaseBsReportFormProcessor` | `_BaseSecReportFormProcessor` |
| XBRL 加载 | 独立文件发现（`discover_xbrl_files`） | 通过 edgartools `XBRL` 对象 |
| 注册优先级 | 200（主路径） | 190（回退） |
| 章节切分 | `_VirtualSectionProcessorMixin` + BS 全文提取 | `_VirtualSectionProcessorMixin` + edgartools sections |
| marker 共享 | 共享 `form_common` 模块 | 共享同一 `form_common` 模块 |
| 搜索增强 | token 级 OR 回退 | 精确短语匹配为主 |

双轨共享层：
- `form_common` 模块（如 `ten_k_form_common.py`）：marker 构建函数纯文本正则，与 HTML 解析引擎无关。
- `_VirtualSectionProcessorMixin`：虚拟章节切分 mixin，双轨共同继承。
- `sec_xbrl_query.py`：XBRL 查询工具函数，BS 路线直接 import 使用。
- `html_financial_statement_common.py` / `report_form_financial_statement_common.py`：财务表结构化，与解析引擎无关。

## 8. 财务报表提取的分层架构

```
financial_base.py (协议层)
  │  FinancialDataProcessor (Protocol)
  │  FinancialStatementResult / XbrlFactsResult / FinancialMeta (TypedDict)
  │  不含任何实现逻辑
  ↓
financial_enhancer.py (标注层)
  │  relabel_tables() → 为每个表格标注 is_financial
  │  is_financial_table() → 基于 caption/headers/context 判定
  │  FinsProcessorMixin → 为通用处理器提供 mixin
  │  不理解报表类型分类，只做"是否金融表"的二元判定
  ↓
html_financial_statement_common.py (结构化核心层)
  │  build_html_statement_result_from_tables() → 主入口
  │  select_html_statement_tables_by_row_signals() → 候选表筛选
  │  normalize_numeric_separators() → 数值清洗
  │  期间列检测、header 识别、行标签匹配
  │  不理解表单类型，所有控制参数通过显式注入
  ↓
report_form_financial_statement_common.py (报告类语义层)
  │  REPORT_FORM_SUPPORTED_STATEMENT_TYPES
  │  select_report_statement_tables() → 报告类候选表筛选
  │  should_apply_report_statement_html_fallback() → fallback 判定
  │  报表类型分类正则（caption/headers/context 评分）
  ↓
sec_xbrl_query.py (XBRL 查询层)
  │  _query_facts_rows() → XBRL facts 查询
  │  _build_statement_rows() → 报表行构建
  │  build_statement_locator() → 报表定位器
  │  currency/units/scale/taxonomy 推断
  ↓
处理器层（消费以上五层）
  - SecProcessor.get_financial_statement() → 先 XBRL，失败时 HTML fallback
  - _BaseSecReportFormProcessor.get_financial_statement() → 覆盖父类，增加 report_form 语义
  - _BaseBsReportFormProcessor.get_financial_statement() → 独立 XBRL + report_form 语义
  - BsSixKFormProcessor → 自行管理 XBRL + six_k_form_common 报表分类
```

## 9. 分层关系总览

```
Layer 4: 具体表单处理器（10-K/10-Q/20-F/8-K/6-K/DEF14A/SC13）
  │  定义 _SUPPORTED_FORMS, _build_markers()
  │  共享 form_common 模块中的 marker 函数
  ↓
Layer 3: 报告类基类
  │  _BaseBsReportFormProcessor (BS 路线)
  │  _BaseSecReportFormProcessor (edgartools 路线)
  │  提供: supports() 判定, 虚拟章节初始化, XBRL 财务报表能力, HTML fallback
  ↓
Layer 2: 虚拟章节 mixin + 通用增强处理器
  │  _VirtualSectionProcessorMixin (跨双轨共享)
  │  FinsBSProcessor / SecProcessor (双轨基座)
  ↓
Layer 1: Engine 通用处理器
  │  BSProcessor / DoclingProcessor / MarkdownProcessor
  │  DocumentProcessor Protocol
  ↓
Layer 0: Source 协议 + base TypedDicts
```

分层原则：
- Layer 4 的各表单处理器互不依赖，只通过 `form_common` 模块共享 marker 逻辑。
- BS 路线和 edgartools 路线平行，共享 `form_common` 和 `_VirtualSectionProcessorMixin`，但 HTML 解析引擎完全独立。
- `SecProcessor`（priority=120）作为通用 SEC 兜底，处理未被专项处理器覆盖的 SEC 文档类型。
- `FinsBSProcessor`（priority=80）作为通用 HTML 兜底，处理非 SEC 的 HTML 文档。
- `FinsDoclingProcessor`（priority=100）作为 Docling JSON 的默认处理器，用于 A 股/港股/Material 类文档。
