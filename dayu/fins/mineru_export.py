"""MinerU 云端精准解析 API 转换出口。

该模块在 dayu-agent 中封装 MinerU v4 精准解析 API（https://mineru.net/apiManage/docs），
将本地 PDF 字节流转换为 Markdown 字节流，作为 PDF→Markdown 转换引擎的可选实现。

与 :mod:`dayu.fins.docling_export` 一致，本模块是仓库内调用 MinerU 云 API 的**唯一**
收敛点，向上层提供稳定签名：

- :func:`convert_pdf_bytes_to_markdown_bytes`：``(raw_bytes, stream_name) -> markdown_bytes``，
  对齐 ``Callable[[bytes, str], bytes]`` 强类型协议，便于后续 P2 pipeline 泛化时直接复用。

实现链路（本地文件上传，纯异步模型）：

1. ``POST /api/v4/file-urls/batch`` 申请签名上传 URL（返回 ``batch_id`` + ``file_url``）；
2. 对返回的 ``file_url`` 直接 ``PUT`` 文件二进制（按官方要求不设 Content-Type），
   上传完成系统自动提交解析任务，无需再调用提交接口；
3. 轮询 ``GET /api/v4/extract-results/batch/{batch_id}`` 直到 ``done`` / ``failed`` / 超时；
4. 下载 ``full_zip_url`` 的 zip 包，解压（含路径穿越防护）后读取 ``full.md``。

认证：请求头 ``Authorization: Bearer <token>``，token 通过 ``MINERU_API_KEY``
环境变量提供（见 :data:`dayu.contracts.env_keys.MINERU_API_KEY_ENV`）。
"""

from __future__ import annotations

import os
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Callable, TypeAlias, cast

import httpx

from dayu.contracts.env_keys import MINERU_API_KEY_ENV

# MinerU API 响应的 JSON 结构强类型视图。``httpx.Response.json()`` 返回 ``Any``
# 是第三方 SDK 边界，本模块内通过 ``cast`` 收敛到该视图，不外泄到上层。
JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

# 与 Docling 收敛点一致的稳定签名：``(raw_bytes, stream_name) -> markdown_bytes``。
# 显式以位置参数风格暴露，避免 keyword-only 与 ``Callable[[bytes, str], bytes]``
# 协议不兼容。
PdfToMarkdownBytes: TypeAlias = Callable[[bytes, str], bytes]

# ---------------------------------------------------------------------------
# 配置常量（默认值，不做 CLI 可配参数）
# ---------------------------------------------------------------------------

_MINERU_API_BASE_URL = "https://mineru.net"
_FILE_URLS_BATCH_PATH = "/api/v4/file-urls/batch"
_BATCH_RESULT_PATH_PREFIX = "/api/v4/extract-results/batch/"

_MODEL_VERSION = "vlm"
_LANGUAGE = "ch"
_ENABLE_FORMULA = True
_ENABLE_TABLE = True

_POLL_INTERVAL_SECONDS = 5.0
_POLL_TIMEOUT_SECONDS = 600.0
_REQUEST_TIMEOUT_SECONDS = 60.0
_DOWNLOAD_TIMEOUT_SECONDS = 120.0

_STATE_DONE = "done"
_STATE_FAILED = "failed"
_FULL_MARKDOWN_FILENAME = "full.md"

# MinerU 已知错误码 → 用户可读提示（官方文档 https://mineru.net/apiManage/docs 错误码表）。
_MINERU_ERROR_CODE_HINTS: dict[str, str] = {
    "A0202": "Token 错误，请检查 MINERU_API_KEY 是否为有效 token",
    "A0211": "Token 已过期，请在 mineru.net API 管理页面重新创建",
    "-60018": "每日解析任务数量已达上限",
    "-60008": "文件读取超时（网络受限）",
}


class MineruApiError(RuntimeError):
    """MinerU 云端 API 调用失败基类。

    覆盖：API Key 缺失 / 认证失败 / API 错误码 / HTTP 状态码错误 / 网络错误。
    """


class MineruPollTimeoutError(MineruApiError):
    """轮询 MinerU 解析任务超时。"""


class MineruResultError(MineruApiError):
    """MinerU 解析结果异常。

    覆盖：zip 下载或解压失败、zip 内缺少 Markdown、zip 路径穿越等。
    """


__all__ = [
    "MineruApiError",
    "MineruPollTimeoutError",
    "MineruResultError",
    "PdfToMarkdownBytes",
    "convert_pdf_bytes_to_markdown_bytes",
]


def convert_pdf_bytes_to_markdown_bytes(raw_data: bytes, stream_name: str) -> bytes:
    """将 PDF 字节流通过 MinerU 云 API 转换为 Markdown 字节流。

    Args:
        raw_data: PDF 原始字节内容。
        stream_name: 流名称，建议直接传文件名以保留扩展名（MinerU 按后缀路由解析模型）。

    Returns:
        MinerU 解析产出的 Markdown 文本，UTF-8 编码为字节内容。

    Raises:
        MineruApiError: API Key 缺失 / 认证失败 / API 错误码 / HTTP 状态码或网络错误。
        MineruPollTimeoutError: 轮询解析结果超时。
        MineruResultError: 任务解析失败 / zip 下载或解压失败 / zip 缺少 Markdown / 路径穿越。
    """

    api_key = _read_api_key()
    with _build_http_client() as client:
        batch_id, upload_url = _request_upload_url(api_key, stream_name, client)
        _upload_pdf(upload_url, raw_data, client)
        zip_url = _poll_batch_result(api_key, batch_id, client)
        markdown_text = _download_and_extract_markdown(zip_url, client)
    return markdown_text.encode("utf-8")


# ---------------------------------------------------------------------------
# API Key / HTTP 基础设施
# ---------------------------------------------------------------------------


def _read_api_key() -> str:
    """从环境变量读取 MinerU API Key。

    Returns:
        MinerU API Key 字符串。

    Raises:
        MineruApiError: ``MINERU_API_KEY`` 环境变量缺失或为空时抛出。
    """

    api_key = os.environ.get(MINERU_API_KEY_ENV)
    if not api_key:
        raise MineruApiError(
            f"缺少 MinerU API Key，请先设置环境变量 {MINERU_API_KEY_ENV}"
            "（在 mineru.net API 管理页面创建）"
        )
    return api_key


def _build_http_client() -> httpx.Client:
    """创建 MinerU API 专用同步 HTTP 客户端。

    Returns:
        带默认请求超时的 ``httpx.Client``。

    Raises:
        无。
    """

    return httpx.Client(timeout=_REQUEST_TIMEOUT_SECONDS)


def _build_auth_headers(api_key: str) -> dict[str, str]:
    """构造 MinerU API 认证请求头。

    Args:
        api_key: MinerU API Key。

    Returns:
        携带 ``Authorization: Bearer <key>`` 与 JSON Content-Type 的请求头字典。

    Raises:
        无。
    """

    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _send_request(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: JsonObject | None = None,
    content: bytes | None = None,
    timeout: float | None = None,
) -> httpx.Response:
    """发送 HTTP 请求并把传输层 / 状态码错误统一包装为 ``MineruApiError``。

    Args:
        client: 复用的 ``httpx.Client``。
        method: HTTP 方法（``GET`` / ``POST`` / ``PUT``）。
        url: 完整请求 URL。
        headers: 可选请求头。
        json_body: 可选 JSON 请求体（与 ``content`` 二选一，优先于 ``content``）。
        content: 可选原始字节请求体。
        timeout: 可选请求超时覆盖；``None`` 使用客户端默认超时。

    Returns:
        已通过 ``raise_for_status`` 校验的响应对象。

    Raises:
        MineruApiError: HTTP 状态码错误或传输层网络错误时抛出。
    """

    try:
        if json_body is not None:
            response = client.request(
                method, url, headers=headers, json=json_body, timeout=timeout
            )
        else:
            response = client.request(
                method, url, headers=headers, content=content, timeout=timeout
            )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise MineruApiError(
            f"MinerU API HTTP 错误: {exc.response.status_code} {method} {url}"
        ) from exc
    except httpx.HTTPError as exc:
        raise MineruApiError(f"MinerU API 网络错误: {method} {url}: {exc}") from exc
    return response


# ---------------------------------------------------------------------------
# 响应解析
# ---------------------------------------------------------------------------


def _parse_json_object(response: httpx.Response, *, context: str) -> JsonObject:
    """解析 MinerU API 响应的 JSON 对象。

    Args:
        response: 已校验的 HTTP 响应。
        context: 调用场景描述，用于错误信息。

    Returns:
        响应体 JSON 对象（强类型视图）。

    Raises:
        MineruApiError: 响应体不是合法 JSON 或不是 JSON 对象时抛出。
    """

    try:
        payload = response.json()
    except ValueError as exc:
        raise MineruApiError(f"{context}: MinerU 响应不是合法 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise MineruApiError(f"{context}: MinerU 响应不是 JSON 对象")
    return cast(JsonObject, payload)


def _check_api_code(payload: JsonObject, *, context: str) -> None:
    """校验 MinerU API 响应业务码，非零时按已知错误码给出提示。

    Args:
        payload: 已解析的 JSON 对象。
        context: 调用场景描述，用于错误信息。

    Returns:
        无。

    Raises:
        MineruApiError: ``code`` 字段非零或缺失时抛出。
    """

    code = payload.get("code")
    if code == 0:
        return
    raw_msg = payload.get("msg")
    msg_text = raw_msg if isinstance(raw_msg, str) and raw_msg else "未知错误"
    hint = _MINERU_ERROR_CODE_HINTS.get(str(code))
    hint_text = f"，{hint}" if hint else ""
    raise MineruApiError(f"{context}: MinerU API 错误 code={code}{hint_text}（{msg_text}）")


def _extract_data_object(payload: JsonObject, *, context: str) -> JsonObject:
    """提取 MinerU 响应的 ``data`` 对象。

    Args:
        payload: 已解析的 JSON 对象。
        context: 调用场景描述，用于错误信息。

    Returns:
        ``data`` 字段对应的 JSON 对象。

    Raises:
        MineruApiError: ``data`` 缺失或不是对象时抛出。
    """

    data = payload.get("data")
    if not isinstance(data, dict):
        raise MineruApiError(f"{context}: MinerU 响应缺少 data 对象")
    return cast(JsonObject, data)


def _expect_str_field(obj: JsonObject, key: str, *, context: str) -> str:
    """从 JSON 对象中提取非空字符串字段。

    Args:
        obj: 源 JSON 对象。
        key: 字段名。
        context: 调用场景描述，用于错误信息。

    Returns:
        非空字符串字段值。

    Raises:
        MineruApiError: 字段缺失、非字符串或为空时抛出。
    """

    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise MineruApiError(f"{context}: MinerU 响应缺少或非法字段 {key!r}")
    return value


# ---------------------------------------------------------------------------
# 转换链路各阶段
# ---------------------------------------------------------------------------


def _request_upload_url(api_key: str, stream_name: str, client: httpx.Client) -> tuple[str, str]:
    """申请 MinerU 本地文件批量上传的签名 URL。

    单文件转换固定申请一个文件的上传链接。

    Args:
        api_key: MinerU API Key。
        stream_name: 文件名（必须带正确后缀，MinerU 按后缀路由解析模型）。
        client: 复用的 ``httpx.Client``。

    Returns:
        ``(batch_id, upload_url)`` 二元组：任务批次 ID 与单个签名上传 URL。

    Raises:
        MineruApiError: 认证失败 / API 错误码 / HTTP 或网络错误 / 响应结构非法。
    """

    url = f"{_MINERU_API_BASE_URL}{_FILE_URLS_BATCH_PATH}"
    body: JsonObject = {
        "files": [{"name": stream_name}],
        "model_version": _MODEL_VERSION,
        "enable_formula": _ENABLE_FORMULA,
        "enable_table": _ENABLE_TABLE,
        "language": _LANGUAGE,
    }
    response = _send_request(
        client, "POST", url, headers=_build_auth_headers(api_key), json_body=body
    )
    payload = _parse_json_object(response, context="申请上传链接")
    _check_api_code(payload, context="申请上传链接")
    data = _extract_data_object(payload, context="申请上传链接")
    batch_id = _expect_str_field(data, "batch_id", context="申请上传链接")
    file_urls = data.get("file_urls")
    if not isinstance(file_urls, list) or not file_urls:
        raise MineruApiError("申请上传链接: MinerU 响应缺少 file_urls 列表")
    first_url = file_urls[0]
    if not isinstance(first_url, str) or not first_url:
        raise MineruApiError("申请上传链接: MinerU 响应 file_urls 项非法")
    return batch_id, first_url


def _upload_pdf(upload_url: str, pdf_bytes: bytes, client: httpx.Client) -> None:
    """通过签名 URL 上传 PDF 字节流。

    MinerU 签名上传 URL 自带鉴权，不需要再传 ``Authorization``；同时按官方要求
    不要设置 ``Content-Type``，直接 ``PUT`` 文件二进制。上传完成后系统自动提交
    解析任务，无需再调用提交接口。

    Args:
        upload_url: 申请到的签名上传 URL。
        pdf_bytes: PDF 原始字节内容。
        client: 复用的 ``httpx.Client``。

    Returns:
        无。

    Raises:
        MineruApiError: HTTP 状态码错误或网络错误时抛出。
    """

    _send_request(client, "PUT", upload_url, content=pdf_bytes)


def _poll_batch_result(api_key: str, batch_id: str, client: httpx.Client) -> str:
    """轮询 MinerU 批量解析结果直到完成。

    单文件批量任务取 ``data.extract_result`` 第一项的状态进行判断。

    Args:
        api_key: MinerU API Key。
        batch_id: 上传申请返回的任务批次 ID。
        client: 复用的 ``httpx.Client``。

    Returns:
        任务 ``done`` 时返回 ``full_zip_url`` 下载链接。

    Raises:
        MineruPollTimeoutError: 超过 ``_POLL_TIMEOUT_SECONDS`` 仍未完成时抛出。
        MineruResultError: 任务进入 ``failed`` 状态时抛出。
        MineruApiError: 认证失败 / API 错误码 / HTTP 或网络错误 / 响应结构非法。
    """

    url = f"{_MINERU_API_BASE_URL}{_BATCH_RESULT_PATH_PREFIX}{batch_id}"
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while True:
        result = _fetch_batch_result(api_key, url, client)
        state = _expect_str_field(result, "state", context="轮询解析结果")
        if state == _STATE_DONE:
            return _expect_str_field(result, "full_zip_url", context="轮询解析结果")
        if state == _STATE_FAILED:
            raise MineruResultError(_format_failed_state_message(result))
        if time.monotonic() >= deadline:
            raise MineruPollTimeoutError(
                f"轮询 MinerU 解析结果超时（{_POLL_TIMEOUT_SECONDS:g} 秒），batch_id={batch_id}"
            )
        _sleep(_POLL_INTERVAL_SECONDS)


def _fetch_batch_result(api_key: str, url: str, client: httpx.Client) -> JsonObject:
    """查询一次 MinerU 批量解析结果。

    Args:
        api_key: MinerU API Key。
        url: 批量结果查询 URL。
        client: 复用的 ``httpx.Client``。

    Returns:
        单个文件的结果对象（``data.extract_result`` 第一项）。

    Raises:
        MineruApiError: 认证失败 / API 错误码 / HTTP 或网络错误 / 响应结构非法。
    """

    response = _send_request(client, "GET", url, headers=_build_auth_headers(api_key))
    payload = _parse_json_object(response, context="轮询解析结果")
    _check_api_code(payload, context="轮询解析结果")
    data = _extract_data_object(payload, context="轮询解析结果")
    results = data.get("extract_result")
    if not isinstance(results, list) or not results:
        raise MineruApiError("轮询解析结果: MinerU 响应缺少 extract_result 列表")
    first_result = results[0]
    if not isinstance(first_result, dict):
        raise MineruApiError("轮询解析结果: extract_result 项不是 JSON 对象")
    return cast(JsonObject, first_result)


def _format_failed_state_message(result: JsonObject) -> str:
    """格式化 MinerU 任务失败状态信息。

    Args:
        result: 单个文件的结果对象。

    Returns:
        可读的失败描述文本。

    Raises:
        无。
    """

    err_msg = result.get("err_msg")
    detail = err_msg if isinstance(err_msg, str) and err_msg else "未知错误"
    return f"MinerU 解析失败: {detail}"


def _sleep(delay_seconds: float) -> None:
    """按指定秒数休眠（模块级包装便于测试注入）。

    Args:
        delay_seconds: 休眠秒数。

    Returns:
        无。

    Raises:
        无。
    """

    time.sleep(delay_seconds)


# ---------------------------------------------------------------------------
# 结果 zip 下载与解压
# ---------------------------------------------------------------------------


def _download_and_extract_markdown(zip_url: str, client: httpx.Client) -> str:
    """下载 MinerU 结果 zip 并提取 Markdown 文本。

    Args:
        zip_url: ``full_zip_url`` 下载链接。
        client: 复用的 ``httpx.Client``。

    Returns:
        zip 内 ``full.md`` 的文本内容；找不到 ``full.md`` 时回退任意 ``*.md`` 文件。

    Raises:
        MineruResultError: 下载失败 / zip 非法 / 解压失败 / 路径穿越 / 缺少 Markdown。
        MineruApiError: 下载链接的 HTTP 状态码或网络错误时抛出。
    """

    response = _send_request(client, "GET", zip_url, timeout=_DOWNLOAD_TIMEOUT_SECONDS)
    try:
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            zip_path = temp_dir / "result.zip"
            zip_path.write_bytes(response.content)
            _extract_zip_safely(zip_path, temp_dir)
            markdown_path = _find_markdown_file(temp_dir)
            if markdown_path is None:
                raise MineruResultError(
                    "MinerU 结果 zip 中未找到 full.md 或其它 Markdown 文件"
                )
            return markdown_path.read_text(encoding="utf-8")
    except MineruResultError:
        raise
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        raise MineruResultError(f"MinerU 结果 zip 处理失败: {exc}") from exc


def _extract_zip_safely(zip_path: Path, extract_dir: Path) -> None:
    """解压 zip 包并做路径穿越防护。

    Args:
        zip_path: zip 文件路径。
        extract_dir: 解压目标目录。

    Returns:
        无。

    Raises:
        MineruResultError: zip 包含不安全路径（路径穿越）时抛出。
        zipfile.BadZipFile: zip 文件非法时抛出。
    """

    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            target = (extract_dir / member).resolve()
            if not _is_inside_dir(target, extract_dir):
                raise MineruResultError(
                    f"MinerU 结果 zip 包含不安全路径: {member!r}"
                )
        zf.extractall(extract_dir)


def _is_inside_dir(path: Path, directory: Path) -> bool:
    """判断路径是否位于目录之内。

    两侧都先 ``resolve`` 再比较，避免 Windows 下临时目录含 junction /
    symlink 时（如 ``%TEMP%`` 指向其它盘符）路径归一化不一致导致误判。

    Args:
        path: 待检查路径。
        directory: 目标目录。

    Returns:
        路径在目录内返回 ``True``，否则返回 ``False``。

    Raises:
        无。
    """

    try:
        path.resolve().relative_to(directory.resolve())
        return True
    except ValueError:
        return False


def _find_markdown_file(extract_dir: Path) -> Path | None:
    """在解压目录中查找 Markdown 结果文件。

    优先返回 ``full.md``；找不到时回退到任意 ``*.md`` 文件。

    Args:
        extract_dir: 解压目标目录。

    Returns:
        Markdown 文件路径；未找到时返回 ``None``。

    Raises:
        无。
    """

    full_markdown = extract_dir / _FULL_MARKDOWN_FILENAME
    if full_markdown.is_file():
        return full_markdown
    for candidate in sorted(extract_dir.rglob("*.md")):
        if candidate.is_file():
            return candidate
    return None
