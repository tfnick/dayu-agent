"""``dayu-cli convert`` 子命令测试。"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from dayu.cli.arg_parsing import parse_arguments
from dayu.cli.commands.convert import run_convert_command
from dayu.fins.mineru_export import MineruApiError


def _make_args(pdf: str, output: str) -> argparse.Namespace:
    """构造 convert 命令参数对象（与 ``_add_global_args`` 注册的参数对齐）。"""

    return argparse.Namespace(
        command="convert",
        pdf=pdf,
        output=output,
        base="./workspace",
        config=None,
        log_level=None,
        debug=False,
        verbose=False,
        info=False,
        quiet=False,
    )


def test_convert_happy_path_writes_markdown(tmp_path: Path) -> None:
    """正常路径应调用转换函数并把 Markdown 落盘。"""

    pdf_path = tmp_path / "报告.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")
    output_path = tmp_path / "out" / "报告.md"

    with (
        patch("dayu.cli.commands.convert.setup_loglevel"),
        patch(
            "dayu.cli.commands.convert.convert_pdf_bytes_to_markdown_bytes",
            return_value="# Markdown 结果".encode("utf-8"),
        ) as mock_convert,
    ):
        exit_code = run_convert_command(_make_args(str(pdf_path), str(output_path)))

    assert exit_code == 0
    mock_convert.assert_called_once_with(b"%PDF-1.4 fake", "报告.pdf")
    assert output_path.read_text(encoding="utf-8") == "# Markdown 结果"


def test_convert_missing_pdf_returns_error(tmp_path: Path) -> None:
    """PDF 不存在时应返回退出码 1 且不写输出文件、不调用转换函数。"""

    missing_pdf = tmp_path / "missing.pdf"
    output_path = tmp_path / "out.md"

    with (
        patch("dayu.cli.commands.convert.setup_loglevel"),
        patch(
            "dayu.cli.commands.convert.convert_pdf_bytes_to_markdown_bytes"
        ) as mock_convert,
    ):
        exit_code = run_convert_command(_make_args(str(missing_pdf), str(output_path)))

    assert exit_code == 1
    mock_convert.assert_not_called()
    assert not output_path.exists()


def test_convert_creates_output_parent_directory(tmp_path: Path) -> None:
    """输出父目录不存在时应自动创建。"""

    pdf_path = tmp_path / "报告.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")
    output_path = tmp_path / "deep" / "nested" / "报告.md"

    with (
        patch("dayu.cli.commands.convert.setup_loglevel"),
        patch(
            "dayu.cli.commands.convert.convert_pdf_bytes_to_markdown_bytes",
            return_value=b"# ok",
        ),
    ):
        exit_code = run_convert_command(_make_args(str(pdf_path), str(output_path)))

    assert exit_code == 0
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == "# ok"


def test_convert_mineru_error_returns_error(tmp_path: Path) -> None:
    """MinerU API 错误应返回退出码 1 且不写输出文件。"""

    pdf_path = tmp_path / "报告.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")
    output_path = tmp_path / "out.md"

    with (
        patch("dayu.cli.commands.convert.setup_loglevel"),
        patch(
            "dayu.cli.commands.convert.convert_pdf_bytes_to_markdown_bytes",
            side_effect=MineruApiError("缺少 MINERU_API_KEY"),
        ),
    ):
        exit_code = run_convert_command(_make_args(str(pdf_path), str(output_path)))

    assert exit_code == 1
    assert not output_path.exists()


# ---------- argparse 装配 ----------


def test_parse_arguments_supports_convert_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """``convert`` 子命令应能被 argparse 正确解析出 --pdf / --output。"""

    monkeypatch.setattr(
        "sys.argv",
        ["cli.py", "convert", "--pdf", "a.pdf", "--output", "out.md"],
    )

    parsed = parse_arguments()

    assert parsed.command == "convert"
    assert parsed.pdf == "a.pdf"
    assert parsed.output == "out.md"


def test_parse_arguments_convert_requires_pdf_and_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``convert`` 缺少必选参数时应解析失败。"""

    monkeypatch.setattr("sys.argv", ["cli.py", "convert", "--pdf", "a.pdf"])

    with pytest.raises(SystemExit):
        parse_arguments()
