"""``dayu/fins/mineru_export.py`` 单元测试。

覆盖：

- 完整 HTTP 调用序列（申请上传 URL → PUT 上传 → 轮询 done → 下载 zip 取 full.md）；
- 申请上传请求携带 Bearer 认证头与默认解析参数；
- API Key 缺失（auth 错误）；
- MinerU 业务错误码（A0202 / -60018）映射；
- HTTP 状态码错误 / 传输层网络错误；
- 轮询 pending 多次后 done（sleep 被调用）；
- 轮询超时；
- 任务 failed 状态；
- zip 路径穿越防护；
- zip 缺少 full.md 时报错、存在其它 .md 时回退；
- ``Callable[[bytes, str], bytes]`` 位置参数协议。

策略：使用 ``httpx.MockTransport`` 注入 fixture 响应，避免真实网络；
通过 monkeypatch ``_build_http_client`` / ``_sleep`` / 轮询常量控制调用序列。
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import Callable, TypeAlias, cast

import httpx
import pytest

from dayu.contracts.env_keys import MINERU_API_KEY_ENV
from dayu.fins import mineru_export
from dayu.fins.mineru_export import (
    MineruApiError,
    MineruPollTimeoutError,
    MineruResultError,
)

# MinerU API 响应 JSON 的强类型视图，与生产模块保持一致。
JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


# ---------- helpers ----------


def _ok_payload(data: JsonObject) -> JsonObject:
    """构造 MinerU ``code==0`` 响应体。"""

    return {"code": 0, "data": data, "msg": "ok"}


def _build_zip_bytes(files: dict[str, bytes]) -> bytes:
    """构造内存 zip 字节。"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buffer.getvalue()


def _happy_path_handler() -> Callable[[httpx.Request], httpx.Response]:
    """构造完整成功链路的 MockTransport handler。"""

    zip_bytes = _build_zip_bytes(
        {"full.md": "# 贵州茅台 2024 年报\n\n| 科目 | 数值 |\n".encode("utf-8")}
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v4/file-urls/batch":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT" and request.url.host == "upload.example.com":
            return httpx.Response(200)
        if request.method == "GET" and request.url.path == "/api/v4/extract-results/batch/batch-1":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "demo.pdf",
                                "state": "done",
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        if request.method == "GET" and request.url.host == "cdn.example.com":
            return httpx.Response(200, content=zip_bytes)
        return httpx.Response(404)

    return _handler


def _install_mock_transport(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    """安装 MockTransport 客户端并注入 mineru_export 的默认行为。"""

    client = httpx.Client(transport=httpx.MockTransport(handler), timeout=5.0)
    monkeypatch.setattr(mineru_export, "_build_http_client", lambda: client)
    monkeypatch.setattr(mineru_export, "_sleep", lambda _seconds: None)
    monkeypatch.setattr(mineru_export, "_POLL_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(mineru_export, "_POLL_TIMEOUT_SECONDS", 60.0)


# ---------- 成功链路 ----------


def test_convert_happy_path_returns_markdown_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    """完整调用序列应返回 zip 中 full.md 的 UTF-8 字节。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    _install_mock_transport(monkeypatch, _happy_path_handler())

    result = mineru_export.convert_pdf_bytes_to_markdown_bytes(
        b"%PDF-1.4 fake pdf", "demo.pdf"
    )

    assert result == "# 贵州茅台 2024 年报\n\n| 科目 | 数值 |\n".encode("utf-8")


def test_convert_sends_auth_header_and_upload_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """申请上传请求应携带 Bearer 认证头与默认解析参数。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    captured_bodies: list[JsonObject] = []
    captured_auth: list[str | None] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v4/file-urls/batch":
            captured_bodies.append(
                cast(JsonObject, json.loads(request.read().decode("utf-8")))
            )
            captured_auth.append(request.headers.get("Authorization"))
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        if request.url.path == "/api/v4/extract-results/batch/batch-1":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "报告.pdf",
                                "state": "done",
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        return httpx.Response(200, content=_build_zip_bytes({"full.md": b"# ok"}))

    _install_mock_transport(monkeypatch, _handler)

    mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "报告.pdf")

    assert captured_auth == ["Bearer test-token"]
    assert captured_bodies == [
        {
            "files": [{"name": "报告.pdf"}],
            "model_version": "vlm",
            "enable_formula": True,
            "enable_table": True,
            "language": "ch",
        }
    ]


def test_convert_polls_until_done(monkeypatch: pytest.MonkeyPatch) -> None:
    """首次轮询 pending、第二次 done 时应正常完成并调用 sleep。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    sleep_calls: list[str] = []

    def _record_sleep(_seconds: float) -> None:
        sleep_calls.append("sleep")

    def _pending_then_done_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        if request.url.path == "/api/v4/extract-results/batch/batch-1":
            state = "done" if sleep_calls else "pending"
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "demo.pdf",
                                "state": state,
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        return httpx.Response(200, content=_build_zip_bytes({"full.md": b"# done"}))

    # 先安装基础 mock（会把 _sleep 设为 no-op、轮询间隔设为 0），
    # 再覆盖 _sleep 为计数版本 —— monkeypatch 后装者生效。
    _install_mock_transport(monkeypatch, _pending_then_done_handler)
    monkeypatch.setattr(mineru_export, "_sleep", _record_sleep)

    result = mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "demo.pdf")

    assert result == b"# done"
    assert sleep_calls == ["sleep"]


def test_convert_matches_callable_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    """转换函数必须满足 ``Callable[[bytes, str], bytes]`` 位置参数协议。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    _install_mock_transport(monkeypatch, _happy_path_handler())

    fn: mineru_export.PdfToMarkdownBytes = mineru_export.convert_pdf_bytes_to_markdown_bytes
    result = fn(b"%PDF-1.4", "demo.pdf")

    assert isinstance(result, bytes)


# ---------- 错误分支 ----------


def test_convert_raises_when_api_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量缺失时应抛出带明确提示的 ``MineruApiError``。"""

    monkeypatch.delenv(MINERU_API_KEY_ENV, raising=False)
    _install_mock_transport(monkeypatch, _happy_path_handler())

    with pytest.raises(MineruApiError, match="MINERU_API_KEY"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_api_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """业务错误码（A0202）应映射为用户可读提示。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "bad-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={"code": "A0202", "data": {}, "msg": "token error", "trace_id": "t"},
        )

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruApiError, match="A0202"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_quota_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """每日配额错误码（-60018）应映射为用户可读提示。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={"code": -60018, "data": {}, "msg": "quota exceeded", "trace_id": "t"},
        )

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruApiError, match="-60018"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_http_status_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP 401 应包装为 ``MineruApiError``。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(401, json={"code": "A0202", "msg": "unauthorized"})

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruApiError, match="401"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """传输层网络错误应包装为 ``MineruApiError``。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        del request
        raise httpx.ConnectError("connection refused")

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruApiError, match="网络错误"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_poll_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """持续 pending 且超过超时阈值时应抛出 ``MineruPollTimeoutError``。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        return httpx.Response(
            200,
            json=_ok_payload(
                {
                    "batch_id": "batch-1",
                    "extract_result": [
                        {"file_name": "demo.pdf", "state": "pending", "err_msg": ""}
                    ],
                }
            ),
        )

    _install_mock_transport(monkeypatch, _handler)
    # 后装覆盖超时阈值为 0，使首次轮询即触发超时（monkeypatch 后装者生效）。
    monkeypatch.setattr(mineru_export, "_POLL_TIMEOUT_SECONDS", 0.0)

    with pytest.raises(MineruPollTimeoutError):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_failed_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """任务进入 failed 状态时应抛出 ``MineruResultError`` 并携带 err_msg。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        return httpx.Response(
            200,
            json=_ok_payload(
                {
                    "batch_id": "batch-1",
                    "extract_result": [
                        {
                            "file_name": "demo.pdf",
                            "state": "failed",
                            "err_msg": "-60008 文件读取超时",
                        }
                    ],
                }
            ),
        )

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruResultError, match="-60008"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_on_zip_path_traversal(monkeypatch: pytest.MonkeyPatch) -> None:
    """zip 包含路径穿越成员时应抛出 ``MineruResultError``。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    zip_bytes = _build_zip_bytes({"../evil.md": b"# evil"})

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        if request.url.path == "/api/v4/extract-results/batch/batch-1":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "demo.pdf",
                                "state": "done",
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        return httpx.Response(200, content=zip_bytes)

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruResultError, match="不安全路径"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_raises_when_zip_missing_markdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """zip 不含任何 Markdown 文件时应抛出 ``MineruResultError``。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    zip_bytes = _build_zip_bytes({"layout.json": b"{}"})

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        if request.url.path == "/api/v4/extract-results/batch/batch-1":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "demo.pdf",
                                "state": "done",
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        return httpx.Response(200, content=zip_bytes)

    _install_mock_transport(monkeypatch, _handler)

    with pytest.raises(MineruResultError, match="full.md"):
        mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")


def test_convert_falls_back_to_any_markdown_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """zip 中无 full.md 但存在其它 .md 时应回退读取。"""

    monkeypatch.setenv(MINERU_API_KEY_ENV, "test-token")
    zip_bytes = _build_zip_bytes(
        {"content_list.json": b"{}", "other.md": "# 回退内容".encode("utf-8")}
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "file_urls": ["https://upload.example.com/upload/1"],
                    }
                ),
            )
        if request.method == "PUT":
            return httpx.Response(200)
        if request.url.path == "/api/v4/extract-results/batch/batch-1":
            return httpx.Response(
                200,
                json=_ok_payload(
                    {
                        "batch_id": "batch-1",
                        "extract_result": [
                            {
                                "file_name": "demo.pdf",
                                "state": "done",
                                "err_msg": "",
                                "full_zip_url": "https://cdn.example.com/result.zip",
                            }
                        ],
                    }
                ),
            )
        return httpx.Response(200, content=zip_bytes)

    _install_mock_transport(monkeypatch, _handler)

    result = mineru_export.convert_pdf_bytes_to_markdown_bytes(b"%PDF-1.4", "a.pdf")

    assert result == "# 回退内容".encode("utf-8")
