# Research: MinerU 云端精准解析 API（PDF → Markdown）

- **Query**: 调研 MinerU 云端 API（https://mineru.net/apiManage/docs）用于在 dayu-agent（Python）中替代 Docling 做 PDF 转 Markdown：认证方式、上传/创建任务接口、轮询/结果接口、结果格式、限流与计费、官方文档地址、HTTP 客户端选择、完整可运行示例。
- **Scope**: external（官方文档 + GitHub/PyPI 佐证）
- **Date**: 2026-08-12
- **主要来源**: https://mineru.net/apiManage/docs（官方文档主页，2026-08-12 抓取）、PyPI `mineru` 包元数据、GitHub `opendatalab/MinerU` 仓库、社区 Python 集成 `HongdiHe/papermind`（src/services/mineru_api.py）。

---

## 关键结论（TL;DR）

- **认证**：精准解析 API 需要账户级 Token（在 mineru.net 的「API 管理页面」手动创建），通过请求头 `Authorization: Bearer <token>` 传递。不是 env var 也不是签名机制。
- **上传**：精准 API 不支持 multipart 直传。本地文件走「申请上传链接 → PUT 到 OSS」两步：`POST /api/v4/file-urls/batch` 申请签名上传 URL，再对返回的 URL 做 `PUT` 上传，上传完成系统自动提交解析任务。
- **异步模型**：所有接口都是「提交 → 轮询」。单文件 URL 任务返回 `task_id`，批量任务返回 `batch_id`。
- **结果**：结果以 **Zip 包 URL**（`full_zip_url`）返回，zip 内含 `full.md`（Markdown 解析结果）、`*_content_list.json`、`*_model.json`、`layout.json` 等。下载 zip 后取 `full.md` 即得 Markdown。官方文档**未记载** lite 变体接口（社区旧 SDK 曾引用，详见 §5 备注）。
- **限流/计费**：每文件 ≤200MB、≤200 页；单次批量申请 ≤50 个文件；每账号每天 1000 页最高优先级额度（超出降级为低优先级）；Agent 轻量 API 另有 IP 限频（超出返回 HTTP 429）。文档页未公开按量价格表。

---

## 1. 认证

- Token 获取：登录 https://mineru.net 后在 **API 管理页面**手动创建（「需填写token（API管理页面自定创建）」）。这是账户级 API Key，不是临时签名。
- 传递方式：请求头 `Authorization: Bearer <token>`（官方原文：「header头中需要包含 Authorization 字段，格式为 Bearer + 空格 + Token」）。
- 请求示例（官方文档）：
  ```python
  import requests
  token = "API管理页面自定创建的token"
  url = "https://mineru.net/api/v4/extract/task"
  header = {
      "Content-Type": "application/json",
      "Authorization": f"Bearer {token}"
  }
  ```
- 错误码：`A0202` Token 错误（检查是否有 Bearer 前缀）、`A0211` Token 过期。
- 备注：官方文档没有规定 env var 名称，但社区集成（papermind 的 `config_loader`）惯例是把它放进配置项 `mineru.api_key`；dayu-agent 可自行命名为 `MINERU_TOKEN`/`MINERU_API_KEY` 等 env var。官方 `mineru` pip 包的 CLI 用的是 `MINERU_TOKEN`（用于本地 CLI，与云端 API 无关，但可作为命名参考）。

## 2. 上传端点

精准解析 API **不支持 multipart 文件直接上传**，有两个入口：

### 2.1 单文件（远程 URL）创建任务
- `POST https://mineru.net/api/v4/extract/task`
- Header：`Authorization: Bearer <token>`、`Content-Type: application/json`
- Body 字段（JSON）：

| 参数 | 类型 | 必选 | 默认 | 说明 |
|---|---|---|---|---|
| url | string | 是 | - | 文件 URL，支持 pdf/doc/docx/ppt/pptx/xls/xlsx/图片(png/jpg/jpeg/jp2/webp/gif/bmp)/html |
| model_version | string | 否 | pipeline | `pipeline` / `vlm` / `MinerU-HTML`（HTML 文件必须显式指定 `MinerU-HTML`） |
| is_ocr | bool | 否 | false | 是否 OCR（仅 pipeline/vlm 有效） |
| enable_formula | bool | 否 | true | 公式识别（仅 pipeline/vlm；vlm 下只影响行内公式） |
| enable_table | bool | 否 | true | 表格识别（仅 pipeline/vlm 有效） |
| language | string | 否 | ch | 文档语言，取值见 §7 |
| data_id | string | 否 | - | 业务侧数据 ID（≤128 字符，`[A-Za-z0-9_.-]`），用于回查 |
| callback | string | 否 | - | 结果回调 URL（不填则必须轮询；回调体带 checksum=SHA256(uid+seed+content) 与 content） |
| seed | string | 否 | - | callback 签名随机串（用 callback 时必填） |
| extra_formats | [string] | 否 | - | 额外导出 docx/html/latex（markdown/json 是默认导出，无需设置） |
| page_ranges | string | 否 | - | 页码范围，如 `"2,4-6"`；`"2--2"` 表示第 2 页到倒数第 2 页 |
| no_cache | bool | 否 | false | 绕过 URL 内容缓存 |
| cache_tolerance | int | 否 | 900 | 缓存容忍时间（秒） |

- CURL 示例：
  ```
  curl -X POST 'https://mineru.net/api/v4/extract/task' \
    -H 'Authorization: Bearer ***' -H 'Content-Type: application/json' \
    -d '{"url": "https://cdn-mineru.openxlab.org.cn/demo/example.pdf", "model_version": "vlm"}'
  ```
- 响应（`code==0` 成功，返回 `data.task_id`）：
  ```json
  {
    "code": 0,
    "data": { "task_id": "a90e6ab6-44f3-4554-b459-b62fe4c6b436" },
    "msg": "ok",
    "trace_id": "c876cd60b202f2396de1f9e39a1b0172"
  }
  ```

### 2.2 本地文件批量上传（申请上传链接 → PUT）
- `POST https://mineru.net/api/v4/file-urls/batch`
- Header：`Authorization: Bearer <token>`、`Content-Type: application/json`
- Body：
  ```json
  {
    "files": [ {"name": "demo.pdf", "data_id": "abcd"} ],
    "model_version": "vlm",
    "enable_formula": true,
    "enable_table": true,
    "language": "ch"
  }
  ```
  - 顶层参数：`enable_formula` / `enable_table` / `language` / `callback` / `seed` / `extra_formats` / `model_version`（同上表）。
  - `files[]` 每项：`name`（必选，必须带正确后缀）、`is_ocr`（可选）、`data_id`（可选）、`page_ranges`（可选）。
  - **限制：单次申请链接不超过 50 个**。
- 响应（返回 `data.batch_id` + `data.file_urls[]`，每个文件一个签名上传 URL）：
  ```json
  {
    "code": 0,
    "data": {
      "batch_id": "2bb2f0ec-a336-4a0a-b61a-241afaf9cc87",
      "file_urls": ["https://mineru.oss-cn-shanghai.aliyuncs.com/api-upload/***"]
    },
    "msg": "ok",
    "trace_id": "c876cd60b202f2396de1f9e39a1b0172"
  }
  ```
- 上传：对每个 `file_url` 直接 `PUT` 文件二进制（**不要设 Content-Type**，官方 CURL：`curl -X PUT -T /path/to/file.pdf 'https://****'`）。上传 URL 有效期 **24 小时**。上传完成后系统自动扫描并提交解析任务，**无需再调用任何提交接口**。
- 备注：官方还提供 URL 批量入口 `POST /api/v4/extract/task/batch`（body 用 `files[].url` 替代 `name`，同样返回 `batch_id`）。

## 3. 任务创建（是否异步）

- 是，**纯异步**：所有提交接口只返回任务标识，不返回解析内容。
- 单文件 URL 任务 → `data.task_id`（UUID 格式）。
- 批量任务（本地上传/URL 批量）→ `data.batch_id`。
- 不填 `callback` 时，客户端必须轮询任务结果接口；填了 `callback` 则由 MinerU 推送（重复最多 5 次）。

## 4. 轮询 / 结果端点

| 用途 | 方法 + URL | 说明 |
|---|---|---|
| 查询单任务 | `GET https://mineru.net/api/v4/extract/task/{task_id}` | 需 `Authorization: Bearer` |
| 查询批量结果 | `GET https://mineru.net/api/v4/extract-results/batch/{batch_id}` | 需 `Authorization: Bearer` |

- 单任务状态 `data.state`：`pending`（排队中）/ `running`（解析中）/ `converting`（格式转换中）/ `done`（完成）/ `failed`（失败）。
- 批量任务每个文件的状态 `data.extract_result[].state`：额外有 `waiting-file`（等待文件上传，本地上传模式）。
- `done` 时返回 `data.full_zip_url`（zip 下载链接）。`running` 时返回 `data.extract_progress`（`extracted_pages` / `total_pages` / `start_time`）。`failed` 时返回 `data.err_msg`。
- 轮询节奏建议：社区实作用 `poll_interval=5s`、总超时 600s（papermind）；官方示例 interval=3s、timeout=300s。

单任务查询响应示例（running → done）：
```json
// running
{
  "code": 0,
  "data": {
    "task_id": "47726b6e-46ca-4bb9-******",
    "state": "running",
    "err_msg": "",
    "extract_progress": {"extracted_pages": 1, "total_pages": 2, "start_time": "2025-01-20 11:43:20"}
  },
  "msg": "ok", "trace_id": "c876cd60b202f2396de1f9e39a1b0172"
}
// done
{
  "code": 0,
  "data": {
    "task_id": "47726b6e-46ca-4bb9-******",
    "state": "done",
    "full_zip_url": "https://cdn-mineru.openxlab.org.cn/pdf/018e53ad-d4f1-475d-b380-36bf24db9914.zip",
    "err_msg": ""
  },
  "msg": "ok", "trace_id": "c876cd60b202f2396de1f9e39a1b0172"
}
```

批量结果响应示例：
```json
{
  "code": 0,
  "data": {
    "batch_id": "2bb2f0ec-a336-4a0a-b61a-241afaf9cc87",
    "extract_result": [
      {
        "file_name": "example.pdf",
        "state": "done",
        "err_msg": "",
        "full_zip_url": "https://cdn-mineru.openxlab.org.cn/pdf/018e53ad-d4f1-475d-b380-36bf24db9914.zip"
      },
      {
        "file_name": "demo.pdf",
        "state": "running",
        "err_msg": "",
        "extract_progress": {"extracted_pages": 1, "total_pages": 2, "start_time": "2025-01-20 11:43:20"}
      }
    ]
  },
  "msg": "ok", "trace_id": "c876cd60b202f2396de1f9e39a1b0172"
}
```

## 5. 结果格式

- 精准 API 结果是一个 **Zip 包**（`full_zip_url`），非裸 Markdown 文本。zip 内容（对应 MinerU 本地输出约定，官方文档指向 https://opendatalab.github.io/MinerU/reference/output_files/ ）：
  - `full.md` —— Markdown 解析结果（**要取的就是这个文件**）
  - `*_content_list.json` —— 内容列表
  - `*_model.json` —— 模型推理结果
  - `layout.json` —— 中间处理结果（middle.json）
  - 若请求了 `extra_formats` 还会有 docx/html/latex
  - HTML 源文件时：`full.md` + `main.html`（提取后正文 html）
- 取 Markdown 的标准做法：GET 下载 `full_zip_url` → 解压（注意 zip 路径穿越防护，社区实作用 `member_path.startswith(temp_dir)` 检查）→ 找到 `full.md` 读取。
- **「full vs lite」备注**：当前官方文档只记载 `full_zip_url`（full 结果）。老版本/社区 SDK 曾出现过 `lite` 批量结果端点（仅返回 markdown 的轻量结果），但本次抓取的官方文档页面（2026-08-12）中**没有**任何 `lite` 字段或 `/extract-results/lite/` 端点的记载 —— 属未找到，需以 full zip 方案为准，集成时不要依赖 lite。
- Agent 轻量 API（/api/v1/agent/parse/*）则直接返回 `data.markdown_url`（CDN 上的 `full.md` 链接），无 zip —— 这是两条完全不同的 API 家族，dayu-agent 若要完整表格/公式能力应使用 v4 精准 API。

## 6. 限流 / 配额 / 计费

- 精准解析 API（v4）：
  - 单文件大小 ≤ **200MB**；单文件页数 ≤ **200 页**。
  - 单次批量申请上传链接 ≤ **50 个文件**。
  - 每个账号每天 **1000 页最高优先级解析额度**；超过 1000 页的部分优先级降低（排队更慢），错误码 `-60018` 每日解析任务数量已达上限。
  - 文档页未写明并发 QPS 上限与按量价格。
- Agent 轻量 API（v1）：文件 ≤ **10MB**、≤ **20 页**；**IP 限频**防滥用，超出返回 HTTP **429**；仅输出 Markdown，固定 pipeline 轻量模型。
- 计费模型：本次抓取的 API 文档页只提到每日 1000 页免费额度，**未公开具体价格表**（mineru.net/pricing 返回 404，价格页未找到）；云端按量付费的具体价格需在站内个人中心/API 管理页确认。→ 集成时按「每天 1000 页高优额度 + 超出降级」的配额模型做重试/降级设计。
- 网络限制：github、aws 等国外 URL 会请求超时（`-60008` 文件读取超时）。

## 7. 官方文档页地址清单（https://mineru.net/apiManage/docs）

- 首页（模式对比 + 精准 API + Agent 轻量 API 全文档）：`https://mineru.net/apiManage/docs`
- 精准 API - 单文件 URL 创建任务：`POST /api/v4/extract/task`（文档内「1.单个文件解析 → 创建解析任务」节）
- 精准 API - 单任务查询：`GET /api/v4/extract/task/{task_id}`（文档内「获取任务结果」节）
- 精准 API - 本地批量上传：`POST /api/v4/file-urls/batch`（文档内「本地文件批量上传解析」节）
- 精准 API - URL 批量创建：`POST /api/v4/extract/task/batch`（文档内「url 批量上传解析」节）
- 精准 API - 批量结果：`GET /api/v4/extract-results/batch/{batch_id}`（文档内「批量获取任务结果」节）
- 精准 API - 错误码表（A0202/A0211/-500/-10001…-60022）
- Agent 轻量 API：`POST /api/v1/agent/parse/url`、`POST /api/v1/agent/parse/file`、`GET /api/v1/agent/parse/{task_id}`（免 Token，IP 限频）
- language 取值参考（同一页面内）：`ch`（默认）/`ch_server`/`en`/`japan`/`korean`/`chinese_cht`/`ta`/`te`/`ka`/`el`/`th` 及语系包 `latin`/`arabic`/`cyrillic`/`east_slavic`/`devanagari`/`greek`/`hebrew`/`korean_hanja` 等。
- 结果 zip 内容说明（MinerU 本地文档）：`https://opendatalab.github.io/MinerU/reference/output_files/`

## 8. HTTP 客户端推荐 / 官方 SDK

- **没有官方云端 Python SDK**：PyPI 上 `mineru-sdk`（404，不存在）；PyPI 的 `mineru` 包（最新稳定 3.4.4，另有 4.0.0a1–a5 预发布）是**本地 CLI + 模型库**（含 `mineru/cli/api_client.py` 等，是自托管服务的客户端），**不是**云端 API SDK。GitHub 搜 `mineru sdk` 只有社区项目（`longcipher/mineru-sdk-rs` Rust、`AoManoh/Mineru-easy-mcp` MCP、`HongdiHe/papermind` Python）。
- 推荐：dayu-agent 若已用 `httpx`，直接自封装 v4 接口即可（三个动词：POST 建任务、PUT 传文件、GET 轮询 + GET 下载 zip），请求均为简单 JSON/REST，无需特殊 client。官方文档示例用 `requests`，社区（papermind）用 `requests` + 指数退避重试。
- 可参考的真实社区实现：`HongdiHe/papermind` 的 `src/services/mineru_api.py`（申请上传 URL → PUT → 轮询 batch → 下载 zip → 解压找 .md），其用法与官方文档完全一致。

## 9. 完整可运行示例（精准 API，本地文件 → Markdown 文本）

```
# 0) 前置：mineru.net 登录 → API 管理页面创建 token
TOKEN="API管理页面自定创建的token"

# 1) 申请批量上传链接（返回 batch_id + file_urls）
curl -X POST 'https://mineru.net/api/v4/file-urls/batch' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"files":[{"name":"demo.pdf","data_id":"my-file-1"}],"model_version":"vlm","enable_formula":true,"enable_table":true,"language":"ch"}'
# → {"code":0,"data":{"batch_id":"2bb2f0ec-...","file_urls":["https://mineru.oss-cn-shanghai.aliyuncs.com/api-upload/***"]},"msg":"ok",...}

# 2) PUT 上传文件（不设 Content-Type；上传后自动提交解析任务）
curl -X PUT -T demo.pdf 'https://mineru.oss-cn-shanghai.aliyuncs.com/api-upload/***'

# 3) 轮询批量结果直到全部 done（每 5s 一次）
curl -X GET "https://mineru.net/api/v4/extract-results/batch/2bb2f0ec-..." -H "Authorization: Bearer $TOKEN"
# → data.extract_result[0].state: pending|waiting-file|running|converting|done|failed
#   done 时 → data.extract_result[0].full_zip_url = "https://cdn-mineru.openxlab.org.cn/pdf/018e53ad-....zip"

# 4) 下载 zip → 解压 → 读 full.md
curl -O 'https://cdn-mineru.openxlab.org.cn/pdf/018e53ad-....zip'
# unzip 018e53ad-....zip   # 内含 full.md 等
```

Python 侧 zip 解压 + 取 markdown 的关键逻辑（参考社区实现并补路径防护）：
```python
import zipfile, tempfile, pathlib, httpx

def fetch_markdown(zip_url: str) -> str:
    with tempfile.TemporaryDirectory() as td:
        resp = httpx.get(zip_url, timeout=120)
        resp.raise_for_status()
        zip_path = pathlib.Path(td) / "result.zip"
        zip_path.write_bytes(resp.content)
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.namelist():
                target = (pathlib.Path(td) / member).resolve()
                if not str(target).startswith(td):
                    raise ValueError(f"unsafe zip path: {member}")
            zf.extractall(td)
        md = next(pathlib.Path(td).rglob("full.md"), None)
        if md is None:  # 兜底：任意 .md
            md = next(pathlib.Path(td).rglob("*.md"), None)
        if md is None:
            raise FileNotFoundError("no markdown in mineru result zip")
        return md.read_text(encoding="utf-8")
```

---

## Caveats / Not Found

- **lite 结果接口**：当前官方文档未见 `/api/v4/extract-results/lite/{batch_id}` 或 `lite` 字段，未确认；不要依赖。
- **价格表**：官方 API 文档只写「每账号每天 1000 页最高优先级额度」，按量价格未公开（mineru.net/pricing 404）。
- **并发 QPS 上限**：精准 API 文档未写明，仅 Agent API 有 IP 限频（429）。
- **官方 SDK**：`mineru_sdk` / `mineru-sdk` 在 PyPI 不存在；PyPI `mineru` 包是本地 CLI/模型库，非云端 SDK。
- 文档页面为 2026-08-12 抓取，接口版本 v4（精准）/ v1（Agent）；接口可能演进，集成时以官网最新为准。
