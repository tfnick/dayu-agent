"""``dayu-cli convert`` 子命令实现。

模块职责：
- 读取本地 PDF 文件字节；
- 调用 :func:`dayu.fins.mineru_export.convert_pdf_bytes_to_markdown_bytes`
  通过 MinerU 云 API 转换为 Markdown；
- 将结果写入指定输出路径（父目录不存在时自动创建）。
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dayu.cli.dependency_setup import setup_loglevel
from dayu.fins.mineru_export import (
    MineruApiError,
    convert_pdf_bytes_to_markdown_bytes,
)
from dayu.log import Log

MODULE = "APP.CONVERT"


def run_convert_command(args: argparse.Namespace) -> int:
    """执行 ``dayu-cli convert`` 子命令。

    Args:
        args: 解析后的命令行参数，包含 ``pdf``（本地 PDF 路径）与
            ``output``（Markdown 输出路径）。

    Returns:
        退出码，0 表示成功，1 表示失败。

    Raises:
        无。所有异常都在命令内部捕获并按失败码上报。
    """

    setup_loglevel(args)
    pdf_path = Path(args.pdf)
    output_path = Path(args.output)
    try:
        _validate_pdf_path(pdf_path)
        raw_data = pdf_path.read_bytes()
        markdown_bytes = convert_pdf_bytes_to_markdown_bytes(raw_data, pdf_path.name)
        _write_output(output_path, markdown_bytes)
    except MineruApiError as exc:
        Log.error(f"转换失败: {exc}", module=MODULE)
        return 1
    except OSError as exc:
        Log.error(f"文件操作失败: {exc}", module=MODULE)
        return 1
    except Exception as exc:
        Log.error(f"转换命令执行失败: {exc}", module=MODULE)
        return 1
    Log.info(f"转换完成: {pdf_path} -> {output_path}", module=MODULE)
    return 0


def _validate_pdf_path(pdf_path: Path) -> None:
    """校验本地 PDF 路径存在且是文件。

    Args:
        pdf_path: 待校验的 PDF 路径。

    Returns:
        无。

    Raises:
        FileNotFoundError: 路径不存在时抛出。
        OSError: 路径存在但不是文件时抛出。
    """

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 文件不存在: {pdf_path}")
    if not pdf_path.is_file():
        raise OSError(f"PDF 路径不是文件: {pdf_path}")


def _write_output(output_path: Path, content: bytes) -> None:
    """将 Markdown 字节写入输出路径，父目录不存在时自动创建。

    Args:
        output_path: 输出文件路径。
        content: 待写入的 Markdown 字节内容。

    Returns:
        无。

    Raises:
        OSError: 目录创建或文件写入失败时抛出。
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(content)
