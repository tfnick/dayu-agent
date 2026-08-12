# Fins Processors 全量文件分析与调用链路

## 1. 总体定位

`dayu/fins/processors/` 是 Fins 领域的文档处理器子包，包含 **39 个 Python 文件**。核心职责是：**把已入库的财报文档（Docling JSON / HTML / Markdown）解析为 LLM 工具可消费的结构化数据**——章节、表格、财务报表、XBRL 数据。

它不是架构分层，而是 Fins 领域包的内部组件，被 `FinsToolService`（工具层）和 pipeline（下载/上传链路）共同消费。

## 2. 文件清单与逐文件职责

### 2.1 包入口与注册表（2 个文件）

| 文件 | 行数 | 主要导出 | 职责 |
|------|------|---------|------|
| `__init__.py` | 34 | 全部处理器类 + 两个 registry 构建函数 | 包入口，统一导出 13 个处理器类 + `build_fins_processor_registry` + `build_bs_experiment_registry` |
| `registry.py` | 190 | `build_fins_processor_registry()`, `build_bs_experiment_registry()` | 在 engine 注册表基础上追加 fins 特化处理器，组装优先级链。`build_bs_experiment_registry` 已退化为 `build_fins_processor_registry` 的别名（BsTenKFormProcessor 已在默认注册表中作为主路径） |

### 2.2 通用增强处理器——覆盖 engine 三大处理器（3 个文件）

| 文件 | 行数 | 主要类 | 职责 |
|------|------|--------|------|
| `fins_docling_processor.py` | 45 | `FinsDoclingProcessor` | 继承 engine `DoclingProcessor`，解析后对表格执行 `relabel_tables` 金融语义标注。用于 A 股/港股等 Docling JSON 文档（priority=100） |
| `fins_bs_processor.py` | 95 | `FinsBSProcessor` | 继承 engine `BSProcessor`，补充金融标注 + SEC layout 表格检测 + EDGAR SGML 信封剥离。是所有 BS 路线表单处理器的基座（priority=80） |
| `fins_markdown_processor.py` | 45 | `FinsMarkdownProcessor` | 继承 engine `MarkdownProcessor`，补充 `relabel_tables` 金融语义标注（priority=100） |

### 2.3 金融数据协议与语义增强（2 个文件）

| 文件 | 行数 | 主要类/函数 | 职责 |
|------|------|------------|------|
| `financial_base.py` | 111 | `FinancialDataProcessor`(Protocol), `FinancialStatementResult`, `XbrlFactsResult`, `FinancialMeta` | 定义金融数据能力协议和 TypedDict，仅 fins 层使用。`FinancialDataProcessor` 协议声明了 `get_financial_statement()` 和 `query_xbrl_facts()` 两个能力 |
| `financial_enhancer.py` | 652 | `FinsProcessorMixin`, `relabel_tables()`, `is_financial_table()`, `extra_financial_table_fields()` | 表格金融语义增强：统一关键词库（中英文 30+ 关键词）、三大财报表证据组（资产负债表/利润表/现金流量表）、判定规则、重标注流程。`FinsProcessorMixin` 为三个通用增强处理器提供共享 mixin |

### 2.4 SEC 通用处理器与工具子模块（6 个文件）

| 文件 | 行数 | 主要类/函数 | 职责 |
|------|------|------------|------|
| `sec_processor.py` | 859 | `SecProcessor` | 基于 edgartools 的 SEC 通用处理器，提供 `list_sections/list_tables/read_section/read_table/search/get_full_text/get_financial_statement/query_xbrl_facts` 全套能力。是 edgartools 路线处理器的基座（priority=120）。对 6-K 显式让位（`supports()` 返回 False） |
| `form_type_utils.py` | 82 | `normalize_form_type()` | SEC 表单类型标准化工具，所有处理器共享。将 `"10K"` → `"10-K"`、`"def 14a"` → `"DEF 14A"` 等 |
| `sec_html_rules.py` | 119 | `strip_edgar_sgml_envelope()`, `is_sec_layout_table()`, `is_sec_cover_page_table()` | SEC/EDGAR HTML 规则真源：layout 表格检测（章节横线表 + 封面页元数据表）、SGML 信封剥离、封面关键词识别 |
| `sec_dom_helpers.py` | 253 | `_extract_text_from_raw_html()`, `_extract_dom_table_contexts()` | SEC 文档 DOM/HTML 解析工具：从原始 HTML 提取纯文本（作为 edgartools `document.text()` 失败时的回退）、基于 DOM 顺序提取每张表格的前文上下文（O(1) 集合查找优化） |
| `sec_section_build.py` | 884 | `_SectionBlock`, `_build_sections()`, `_safe_document_text()` | SEC 文档章节切分与定位：从 edgartools 文档对象构建章节列表，含 TOC 去噪、marker 定位、anchor 序号解析、快速模式、单全文章节模式 |
| `sec_table_extraction.py` | ~2200 | `_build_tables()`, `_render_markdown_table()`, `_render_records_table()`, `_TableBlock`, `_TableDataFrameProvider` | SEC 文档表格提取、渲染、分类全流程：表格构建→章节匹配→消歧→维度/表头提取→财务表判定→records/markdown/HTML 三路径渲染。含幽灵列合并、MultiIndex 展平、index 恢复等 SEC 表格特有处理 |
| `sec_xbrl_query.py` | 798 | `_STATEMENT_METHODS`, `_query_facts_rows()`, `build_statement_locator()`, `_infer_xbrl_taxonomy()` 等 | XBRL 查询与财务报表结构化提取：报表类型映射、taxonomy 推断、facts 查询（concept 精确匹配 + TextBlock 过滤 + 去重）、数值提取与标准化、currency/units/scale 推断 |

### 2.5 虚拟章节公共 mixin（1 个文件）

| 文件 | 行数 | 主要类/函数 | 职责 |
|------|------|------------|------|
| `sec_form_section_common.py` | ~3000 | `_VirtualSectionProcessorMixin`, `_VirtualSection`, `_dedupe_markers()`, `_build_virtual_sections()` 等 | SEC 表单专项章节处理器公共能力：基于全文 marker 的虚拟章节切分 mixin，被全部 14 个表单处理器共享。含 Cover Page 自适应截断、标题剥离、Part 标题裁剪、页码定位符清理、结构化子章节拆分、表格-章节双向映射（两阶段：标题匹配 + 位置回退）、token OR 搜索回退 |

### 2.6 报告类表单基类（双轨）（4 个文件）

| 文件 | 行数 | 主要类 | 职责 |
|------|------|--------|------|
| `sec_report_form_common.py` | 1408 | `_BaseSecReportFormProcessor` | edgartools 路线报告类基类，继承 `SecProcessor`。提供 TOC 去噪（连续短 span 检测 + 部分 ToC 检测 + 全局比例检测）、Item marker 顺序选取（贪心游标 + 行内引用过滤 + 重试迭代）、XBRL + HTML fallback、快速章节构建模式（`_ENABLE_FAST_SECTION_BUILD=True`） |
| `bs_report_form_common.py` | 506 | `_BaseBsReportFormProcessor` | BS 路线报告类基类，继承 `FinsBSProcessor`。独立加载 XBRL（通过 `discover_xbrl_files`，不依赖 edgartools 文档对象），提供与 edgartools 路线平行的 `get_financial_statement` / `query_xbrl_facts` 能力。复用 `_VirtualSectionProcessorMixin` 虚拟章节切分，复用 `sec_xbrl_query` 工具函数 |
| `html_financial_statement_common.py` | ~1800 | `build_html_statement_result_from_tables()`, `select_html_statement_tables_by_row_signals()`, `normalize_numeric_separators()` | HTML 财务报表结构化共享核心，与表单类型无关。处理 DataFrame→矩阵→表头推断→期间列检测→数值列识别→行标签匹配→期间签名分组→去重→结果构建全链路。含多语言日期解析（英/西/法/葡）、货币识别、scale 推断、单期间摘要表回退 |
| `report_form_financial_statement_common.py` | 514 | `REPORT_FORM_SUPPORTED_STATEMENT_TYPES`, `select_report_statement_tables()`, `classify_report_statement_type_for_table()` | 报告类表单财务表语义层：5 种报表类型（income/balance_sheet/cash_flow/equity/comprehensive_income）的分类规则（caption/headers/context 评分制）、候选表筛选顺序（layout 排除→is_financial 分类→row-signal fallback→relaxed row-signal）、噪声表排除（目录/附注/封面） |

### 2.7 表单公共常量模块——marker 共享层（7 个文件）

| 文件 | 行数 | 主要导出 | 职责 |
|------|------|---------|------|
| `ten_k_form_common.py` | 1893 | `_TEN_K_ITEM_ORDER`, `_build_ten_k_markers()`, `expand_ten_k_virtual_sections_content()` | 10-K 共享常量与 marker 构建：21 个法定 Item（1~15 + 1A/1B/1C/7A/9A/9B/9C）顺序选取、TOC 检测与跳过、行内交叉引用过滤、Item 描述扩展、`incorporated by reference` 正文展开 |
| `ten_q_form_common.py` | 1564 | `_TEN_Q_ITEM_PATTERN`, `_build_ten_q_markers()`, `expand_ten_q_virtual_sections_content()` | 10-Q 共享常量与 marker 构建：两阶段 Item 选取（Part I Items 1-4 + Part II Items 1-6+1A）、SEC 法定 Part 标题锚定、possessive 匹配（Management's/Managements） |
| `twenty_f_form_common.py` | 3125 | `_TWENTY_F_ITEM_ORDER`, `_build_twenty_f_markers()`, `_select_preferred_twenty_f_text()` | 20-F 共享常量与 marker 构建：56 个 Item（1~19 + 16A-16J + 18.A-18.K 子项）、cross-reference 检测与修复、reference guide 识别、context contamination 验证、全文质量选取策略 |
| `eight_k_form_common.py` | 61 | `_EIGHT_K_ITEM_PATTERN`, `_build_eight_k_markers()` | 8-K 共享常量与 marker 构建：`Item X.XX` 格式匹配 + SIGNATURE 尾段 |
| `six_k_form_common.py` | 1959 | `_build_six_k_markers()`, `_classify_statement_type_for_table()`, `_build_statement_result_from_tables()` | 6-K 共享常量、marker 与报表分类：语义关键词章节切分（Exhibit→Financial Results→Safe Harbor→...）、6-K 专属报表类型分类、OCR 页面财务表提取、行信号阈值 |
| `def14a_form_common.py` | 165 | `_DEF14A_SECTION_MARKERS`, `_build_def14a_markers()`, `_select_def14a_proposal_markers()` | DEF 14A 共享常量与 marker 构建：Proposal No. N 为主轴 + 8 个治理章节强标题 + Annex/Appendix/SIGNATURE 尾段 |
| `sc13_form_common.py` | 139 | `_SC13_ITEM_PATTERN`, `_build_sc13_markers()` | SC 13D/G 共享常量与 marker 构建：Item 1-7 顺序选取 + SIGNATURE + Schedule A + Exhibit 尾段 |

### 2.8 BS 路线表单处理器——priority 200，主路径（7 个文件）

| 文件 | 行数 | 主要类 | 继承 | 职责 |
|------|------|--------|------|------|
| `bs_ten_k_processor.py` | 109 | `BsTenKFormProcessor` | `_BaseBsReportFormProcessor` | BS 路线 10-K 处理器，共享 `_build_ten_k_markers()` |
| `bs_ten_q_processor.py` | 106 | `BsTenQFormProcessor` | `_BaseBsReportFormProcessor` | BS 路线 10-Q 处理器，共享 `_build_ten_q_markers()` |
| `bs_twenty_f_processor.py` | 289 | `BsTwentyFFormProcessor` | `_BaseBsReportFormProcessor` | BS 路线 20-F 处理器，共享 `_build_twenty_f_markers()`，含全文质量评估和 `source_text` 替换策略 |
| `bs_eight_k_processor.py` | 137 | `BsEightKFormProcessor` | `FinsBSProcessor` | BS 路线 8-K 处理器，无 XBRL，token OR 搜索回退 |
| `bs_sc13_processor.py` | 143 | `BsSc13FormProcessor` | `FinsBSProcessor` | BS 路线 SC 13 处理器，无 XBRL，token OR 搜索回退 |
| `bs_six_k_processor.py` | 979 | `BsSixKFormProcessor` | `FinsBSProcessor` | BS 路线 6-K 处理器，含独立 XBRL 加载 + 6-K 专属报表分类 + OCR 页面提取 |
| `bs_def14a_processor.py` | 644 | `BsDef14AFormProcessor` | `FinsBSProcessor` | BS 路线 DEF 14A 处理器，无 XBRL，含 TOC 感知（前导聚簇检测 + 迭代跳过）+ token OR 搜索回退 |

### 2.9 edgartools 路线表单处理器——priority 190，回退路径（6 个文件）

| 文件 | 行数 | 主要类 | 继承 | 职责 |
|------|------|--------|------|------|
| `ten_k_processor.py` | 93 | `TenKFormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 10-K 处理器，共享 `_build_ten_k_markers()` |
| `ten_q_processor.py` | 93 | `TenQFormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 10-Q 处理器，共享 `_build_ten_q_markers()` |
| `twenty_f_processor.py` | 114 | `TwentyFFormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 20-F 处理器，共享 `_build_twenty_f_markers()` |
| `eight_k_processor.py` | 63 | `EightKFormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 8-K 处理器 |
| `sc13_processor.py` | 99 | `Sc13FormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 SC 13 处理器 |
| `def14a_processor.py` | 62 | `Def14AFormProcessor` | `_BaseSecReportFormProcessor` | edgartools 路线 DEF 14A 处理器 |

> **注**：6-K 没有 edgartools 路线回退处理器（`SecProcessor.supports()` 对 6-K 返回 False），BS 路线 `BsSixKFormProcessor` 是唯一实现。

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
- BS 路线非报告类处理器（8-K/SC13/6-K/DEF14A）直接继承 `FinsBSProcessor`，因为它们不需要 XBRL 或自行管理 XBRL（6-K）。
- edgartools 路线全部表单处理器继承 `_BaseSecReportFormProcessor`。
- `_VirtualSectionProcessorMixin` 是双轨共享的虚拟章节切分 mixin。
- BS 路线和 edgartools 路线的表单处理器共享 `form_common` 模块中的 marker 函数，但 HTML 解析引擎完全独立。

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
- **SecProcessor**：检查文件后缀是否为 `.htm/.html/.xhtml/.xml`，form_type 是否在 `_SUPPORTED_FORMS` 中（6-K 显式排除）。

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
  → build_fins_processor_registry()            ← registry.py
    → build_engine_processor_registry()         ← engine 基座
    → 覆盖注册 fins 增强处理器                   ← 覆盖 engine
    → 注册 SEC 专项处理器（双轨）                ← BS(200) + edgartools(190)
    → 注册 SecProcessor                         ← 通用兜底(120)
  → ProcessorRegistry 实例存入 FinsRuntime
```

调用方：
- `dayu/fins/service_runtime.py:1314` — `DefaultFinsRuntime` 初始化时调用
- `dayu/fins/pipelines/factory.py:51` — pipeline 工厂调用（用于 snapshot export 等场景）

### 6.2 运行时工具调用

```
LLM tool call: search_document(ticker="AAPL", document_id="fil_xxx", query="risk factors")
  │
  ├─ FinsToolService.search_document(ticker, document_id, query)
  │    dayu/fins/tools/service.py
  │
  ├─ _get_or_create_processor(ticker, document_id)       ← service.py:1727
  │    ├─ 检查 ProcessorLRUCache (key = ticker + document_id)
  │    │   命中 → 返回缓存实例
  │    │
  │    └─ _create_processor(ticker, document_id)         ← service.py:1761
  │         ├─ _resolve_source_kind(ticker, document_id)  ← 判定 filing/material
  │         ├─ source_repository.get_primary_source(...)  ← 获取 Source 对象
  │         ├─ source_repository.get_source_meta(...)     ← 读取 meta.json
  │         │   → 获取 form_type
  │         └─ processor_registry.create_with_fallback(   ← service.py:1785
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
       → 精确短语正则匹配
       → (若启用 token fallback) token OR 回退
       → 返回 SearchHit 列表
```

FinsToolService 中所有 8 个工具方法都通过 `_get_or_create_processor` 获取处理器实例：

| 工具方法 | service.py 行号 | 调用的 processor 方法 |
|---------|----------------|---------------------|
| `list_sections` | 287 | `processor.list_sections()` |
| `read_section` | 334 | `processor.read_section(ref)` |
| `list_tables` | 489 | `processor.list_tables()` |
| `read_table` | 803 | `processor.read_table(table_ref)` |
| `search` | 894 | `processor.search(query, within_ref)` |
| `get_financial_statement` | 986 | `processor.get_financial_statement(...)` |
| `query_xbrl_facts` | 1057 | `processor.query_xbrl_facts(...)` |
| `get_xbrl_taxonomy` | 1141 | `processor.get_xbrl_taxonomy()` |

### 6.3 上传/下载链路中的 processor 调用

```
upload_filing / cn_download pipeline
  → DoclingUploadService / cn_download_filing_workflow
    → 不直接调用 ProcessorRegistry
    → 只负责把 PDF 转成 Docling JSON 并落盘
    → ProcessorRegistry 在后续工具调用时才被使用
```

**关键设计**：processor 不参与文档入库流程，仅在 LLM 工具调用时按需创建。ProcessorLRUCache 保证同一 `(ticker, document_id)` 的 processor 实例被复用，避免重复解析。

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
| 搜索增强 | token 级 OR 回退（8-K/6-K/DEF14A/SC13） | 精确短语匹配为主 |
| 全文提取 | `BSProcessor.get_full_text()`（BeautifulSoup `get_text`） | `SecProcessor.get_full_text()`（edgartools `document.text()` + HTML 回退） |
| 表格渲染 | DataFrame→records（`parse_html_table_dataframe`） | edgartools `to_dataframe()` + records/markdown/HTML 三路径 |

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
  │  多语言日期解析（英/西/法/葡）、货币识别、scale 推断
  │  不理解表单类型，所有控制参数通过显式注入
  ↓
report_form_financial_statement_common.py (报告类语义层)
  │  REPORT_FORM_SUPPORTED_STATEMENT_TYPES
  │  select_report_statement_tables() → 报告类候选表筛选
  │  classify_report_statement_type_for_table() → 报表类型分类
  │  should_apply_report_statement_html_fallback() → fallback 判定
  │  报表类型分类正则（caption/headers/context 评分制，6/4/1 权重）
  │  噪声表排除（目录/附注/封面/签名等 13 个模式）
  ↓
sec_xbrl_query.py (XBRL 查询层)
  │  _query_facts_rows() → XBRL facts 查询
  │  _build_statement_rows() → 报表行构建
  │  build_statement_locator() → 报表定位器
  │  currency/units/scale/taxonomy 推断
  │  concept 精确本地名匹配 + TextBlock 过滤 + 去重
  ↓
处理器层（消费以上五层）
  - SecProcessor.get_financial_statement() → 先 XBRL，失败时 HTML fallback
  - _BaseSecReportFormProcessor.get_financial_statement() → 覆盖父类，增加 report_form 语义
  - _BaseBsReportFormProcessor.get_financial_statement() → 独立 XBRL + report_form 语义
  - BsSixKFormProcessor → 自行管理 XBRL + six_k_form_common 报表分类
```

### 财务报表提取的 fallback 链

```
get_financial_statement(statement_type="income")
  │
  ├─ 尝试 XBRL 路径
  │    ├─ _get_xbrl() → 延迟加载 XBRL 对象
  │    │   ├─ discover_xbrl_files(source_path.parent) → 发现 .xml/.xsd 文件
  │    │   └─ XBRL.from_files(...) → 构建 XBRL 对象
  │    ├─ xbrl.statements.income_statement() → 获取报表对象
  │    ├─ statement_obj.to_dataframe() → 转 DataFrame
  │    ├─ _extract_period_columns() → 识别期末列
  │    ├─ _build_statement_rows() → 构建标准行
  │    ├─ _infer_units/currency/scale() → 推断单位/货币/ scale
  │    └─ build_statement_locator() → 构建定位信息
  │    → 成功 → 返回 FinancialStatementResult (data_quality="xbrl")
  │
  └─ XBRL 失败 → HTML fallback（仅报告类表单）
       ├─ should_apply_report_statement_html_fallback(reason) → 判定是否允许 fallback
       ├─ select_report_statement_tables() → 筛选候选 HTML 表
       │    ├─ 排除 layout 表
       │    ├─ 在 is_financial=True 的表中做 caption/header/context 分类
       │    ├─ 若无命中 → row-signal fallback（行标签关键词匹配）
       │    └─ 若仍无命中 → relaxed row-signal fallback（更宽松的词表 + 更高阈值）
       ├─ build_html_statement_result_from_tables() → 结构化
       │    ├─ 每张表 DataFrame→矩阵→表头推断→期间列检测→行构建
       │    ├─ 按期间签名分组聚合
       │    ├─ 选择行数最多/表数最多的组
       │    └─ 去重、构建期间摘要、推断货币/scale
       └─ 返回 FinancialStatementResult (data_quality="extracted")
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
  │  FinsDoclingProcessor / FinsMarkdownProcessor (通用增强)
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

## 10. 文件依赖关系图

```
__init__.py
  └─ registry.py
       ├─ fins_docling_processor.py ─→ financial_enhancer.py
       ├─ fins_markdown_processor.py ─→ financial_enhancer.py
       ├─ fins_bs_processor.py ─→ financial_enhancer.py, sec_html_rules.py
       ├─ bs_*_processor.py (7 个) ─→ bs_report_form_common.py / fins_bs_processor.py
       │    │                         + 对应的 *_form_common.py
       │    └─ bs_report_form_common.py ─→ sec_form_section_common.py, sec_xbrl_query.py,
       │                                   html_financial_statement_common.py,
       │                                   report_form_financial_statement_common.py
       ├─ *_processor.py (6 个) ─→ sec_report_form_common.py
       │    │                      + 对应的 *_form_common.py
       │    └─ sec_report_form_common.py ─→ sec_form_section_common.py, sec_processor.py,
       │                                     sec_section_build.py, sec_table_extraction.py,
       │                                     html_financial_statement_common.py,
       │                                     report_form_financial_statement_common.py
       ├─ sec_processor.py ─→ sec_xbrl_query.py, sec_section_build.py,
       │                      sec_dom_helpers.py, sec_table_extraction.py,
       │                      sec_html_rules.py, form_type_utils.py, financial_base.py
       └─ sec_form_section_common.py ─→ sec_processor.py (仅 Protocol cast, 非强依赖)

独立工具模块（被多文件引用）：
  - form_type_utils.py        ← 所有表单处理器
  - financial_base.py          ← sec_processor, bs_report_form_common, sec_xbrl_query
  - financial_enhancer.py      ← fins_docling/bs/markdown_processor
  - sec_html_rules.py          ← fins_bs_processor, sec_table_extraction
  - sec_dom_helpers.py         ← sec_processor
  - sec_section_build.py       ← sec_processor, sec_report_form_common
  - sec_table_extraction.py    ← sec_processor, sec_report_form_common, bs_report_form_common
  - sec_xbrl_query.py          ← sec_processor, bs_report_form_common, bs_six_k_processor
  - html_financial_statement_common.py ← sec_report_form_common, bs_report_form_common, six_k_form_common
  - report_form_financial_statement_common.py ← sec_report_form_common, bs_report_form_common
```

## 11. 关键设计决策总结

1. **双轨设计**：BS 路线（BeautifulSoup）作为主路径（priority=200），edgartools 路线作为回退（priority=190）。BS 路线不依赖 edgartools 黑箱，HTML 解析完全可控；edgartools 路线作为备选，在 BS 路线实例化失败时自动接管。

2. **虚拟章节切分**：所有 SEC 表单处理器通过 `_VirtualSectionProcessorMixin` 实现基于全文 marker 的章节切分，而非依赖底层解析引擎的 section 结构。这保证双轨路线产出一致的章节结构。

3. **marker 共享**：每种表单的 marker 构建函数（`_build_xxx_markers`）抽取到独立的 `xxx_form_common.py` 模块，与 HTML 解析引擎无关，BS 路线和 edgartools 路线共享同一套 marker 逻辑。

4. **XBRL 独立加载**：BS 路线通过 `discover_xbrl_files()` 独立发现 XBRL 文件并构建 `XBRL` 对象，不经过 edgartools `HTMLParser`。XBRL 查询工具函数（`sec_xbrl_query.py`）双轨共享。

5. **财务报表 HTML fallback**：当 XBRL 不可用时，报告类表单处理器自动降级到 HTML 表格结构化提取。降级链：XBRL → caption/header 分类 → row-signal 匹配 → relaxed row-signal 匹配。

6. **ProcessorLRUCache**：`FinsToolService` 内置 LRU 缓存，同一 `(ticker, document_id)` 的 processor 实例被复用，避免重复解析大文档。

7. **6-K 特殊处理**：6-K 没有 edgartools 路线回退（`SecProcessor.supports()` 对 6-K 返回 False），因为 edgartools 对 6-K 的分段结果在部分材料型文档上给 LLM 产生低质量输入。BS 路线 `BsSixKFormProcessor` 是唯一实现，且包含独立的 XBRL 加载和 6-K 专属报表分类逻辑。
