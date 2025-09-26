from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use("Agg")

plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "SimHei",
    "Noto Sans CJK SC",
    "Source Han Sans SC",
    "STHeiti",
    "PingFang SC",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


@dataclass(slots=True)
class ChartGenerationResult:
    row_count: int
    columns_used: List[str]
    encoding: str


_PREFERRED_COLUMNS: List[str] = [
    "钻头深度(m)",
    "立压log(MPa)",
    "泵冲1(spm)",
    "泵冲2(spm)",
    "泵冲3(spm)",
    "入口流量(L/s)",
    "FDT101(L/s)",
]

_ENCODINGS: Iterable[str] = ("gbk", "utf-8", "utf-8-sig")


def detect_encoding(file_path: Path) -> str:
    last_error: Exception | None = None
    for encoding in _ENCODINGS:
        try:
            pd.read_csv(file_path, nrows=0, encoding=encoding)
            return encoding
        except Exception as exc:  # pragma: no cover - errors will be retried
            last_error = exc
            continue
    raise RuntimeError(f"无法读取文件 {file_path.name}: {last_error}")


def _select_columns(file_path: Path, encoding: str) -> List[str]:
    header_df = pd.read_csv(file_path, nrows=0, encoding=encoding)
    columns = list(header_df.columns)
    preferred = [col for col in _PREFERRED_COLUMNS if col in columns]
    if preferred:
        return preferred[:3]

    sample_df = pd.read_csv(file_path, nrows=2000, encoding=encoding)
    numeric_cols = sample_df.select_dtypes(include="number").columns.tolist()
    return numeric_cols[:3]


def generate_chart_from_csv(
    file_path: Path,
    output_path: Path,
    *,
    sample_points: int = 5000,
    chunk_size: int = 20000,
) -> ChartGenerationResult:
    """Generate a lightweight chart for a large CSV file and return row metadata."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    encoding = detect_encoding(file_path)
    selected_columns = _select_columns(file_path, encoding)

    if not selected_columns:
        # Fallback to any available column names by reading the first chunk
        with pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size) as reader:
            first_chunk = next(reader)
            numeric_cols = first_chunk.select_dtypes(include="number").columns.tolist()
            if numeric_cols:
                selected_columns = numeric_cols[:3]
            else:
                selected_columns = first_chunk.columns[:3].tolist()

    total_rows = 0
    sampled_rows = 0
    offset = 0
    sampled_frames: List[pd.DataFrame] = []

    with pd.read_csv(
        file_path,
        usecols=lambda col: col in selected_columns,
        encoding=encoding,
        chunksize=chunk_size,
    ) as reader:
        for chunk in reader:
            chunk_length = len(chunk)
            total_rows += chunk_length

            numeric_chunk = chunk.select_dtypes(include="number")
            if numeric_chunk.empty:
                offset += chunk_length
                continue

            # ensure consistent column order
            chunk = numeric_chunk.reindex(columns=selected_columns, fill_value=np.nan)

            if sampled_rows < sample_points:
                remaining = sample_points - sampled_rows
                take = min(len(chunk), remaining)
                if take > 0:
                    indices = np.linspace(0, len(chunk) - 1, take, dtype=int)
                    selected_chunk = chunk.iloc[indices].copy()
                    selected_chunk.insert(0, "row_number", offset + indices + 1)
                    sampled_frames.append(selected_chunk)
                    sampled_rows += len(selected_chunk)

            offset += chunk_length

    if not sampled_frames:
        fig, ax = plt.subplots(figsize=(12, 4), dpi=120)
        ax.text(0.5, 0.5, "无可视化数据", ha="center", va="center", fontsize=14)
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(output_path, bbox_inches="tight")
        plt.close(fig)
        return ChartGenerationResult(row_count=total_rows, columns_used=[], encoding=encoding)

    sample_df = pd.concat(sampled_frames, ignore_index=True)
    columns_to_plot = [col for col in selected_columns if col in sample_df.columns]
    if not columns_to_plot:
        columns_to_plot = [col for col in sample_df.columns if col != "row_number"]

    colors = ["#3B82F6", "#EF4444", "#10B981"]
    fig, axes = plt.subplots(len(columns_to_plot), 1, sharex=True, figsize=(14, 3 * len(columns_to_plot)), dpi=130)

    if not isinstance(axes, np.ndarray):
        axes = np.array([axes])

    x_values = sample_df["row_number"].to_numpy()

    for idx, column in enumerate(columns_to_plot):
        ax = axes[idx]
        ax.plot(x_values, sample_df[column].to_numpy(), color=colors[idx % len(colors)], linewidth=1.3)
        ax.set_ylabel(column)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("行号 (采样)")
    fig.suptitle(file_path.name, fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)

    return ChartGenerationResult(row_count=total_rows, columns_used=columns_to_plot, encoding=encoding)
