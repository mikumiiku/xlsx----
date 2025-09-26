from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, List
from uuid import uuid4

import pandas as pd

from .models import CSVFileInfo, EventRecord, EventType


class AnnotationManager:
    """Centralised state for CSV metadata and user annotations."""

    def __init__(
        self,
        *,
        csv_dir: Path | str = Path("./csv"),
        chart_dir: Path | str = Path("./charts"),
        events_file: Path | str = Path("./marked/events.json"),
        max_chart_points: int = 5000,
        chart_chunk_size: int = 20000,
        chart_subdir: str | None = "综合指标",
    ) -> None:
        self.csv_dir = Path(csv_dir)
        self.chart_dir = Path(chart_dir)
        self.events_file = Path(events_file)
        self.max_chart_points = max_chart_points
        self.chart_chunk_size = chart_chunk_size
        self.chart_subdir = chart_subdir
        self.marked_dir = self.events_file.parent

        self._lock = threading.RLock()
        self._files: List[CSVFileInfo] = []
        self._events: List[EventRecord] = []
        self._file_index: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Startup and persistence helpers
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        with self._lock:
            self.chart_dir.mkdir(parents=True, exist_ok=True)
            if self.chart_subdir:
                (self.chart_dir / self.chart_subdir).mkdir(parents=True, exist_ok=True)
            self.marked_dir.mkdir(parents=True, exist_ok=True)
            self._load_files()
            self._load_events()

    def _load_files(self) -> None:
        csv_files = sorted(
            (path for path in self.csv_dir.glob("Rec*.csv") if path.is_file()),
            key=lambda path: path.name,
        )

        files: List[CSVFileInfo] = []
        for path in csv_files:
            encoding = self._detect_encoding_with_fallback(path)
            row_count = self._count_rows(path)

            chart_path, chart_relative_path = self._resolve_chart_path(path)

            files.append(
                CSVFileInfo(
                    name=path.name,
                    path=path,
                    row_count=row_count,
                    chart_path=chart_path,
                    chart_relative_path=chart_relative_path,
                    encoding=encoding,
                )
            )

        self._files = files
        self._file_index = {info.name: idx for idx, info in enumerate(self._files)}

    def _load_events(self) -> None:
        if not self.events_file.exists():
            self._events = []
            return

        with self.events_file.open("r", encoding="utf-8") as fh:
            raw_events = json.load(fh)
        self._events = [EventRecord(**item) for item in raw_events]

    def _save_events(self) -> None:
        with self.events_file.open("w", encoding="utf-8") as fh:
            json.dump([event.to_dict() for event in self._events], fh, ensure_ascii=False, indent=2)

    def _detect_encoding_with_fallback(self, file_path: Path) -> str:
        return "utf-8"

    def _resolve_chart_path(self, csv_path: Path) -> tuple[Path | None, str]:
        if self.chart_subdir:
            relative = Path(self.chart_subdir) / f"{csv_path.stem}.png"
        else:
            relative = Path(f"{csv_path.stem}.png")
        absolute = self.chart_dir / relative
        if absolute.exists():
            return absolute, relative.as_posix()
        return None, relative.as_posix()

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------
    def list_files(self) -> List[Dict[str, object]]:
        with self._lock:
            return [info.to_public_dict() for info in self._files]

    def list_events(self) -> List[Dict[str, object]]:
        with self._lock:
            return [event.to_dict() for event in self._events]

    # ------------------------------------------------------------------
    # Event manipulation
    # ------------------------------------------------------------------
    def add_event(
        self,
        *,
        event_type: EventType,
        start_file: str,
        start_row: int,
        end_file: str,
        end_row: int,
    ) -> EventRecord:
        with self._lock:
            self._validate_event(event_type, start_file, start_row, end_file, end_row)
            event = EventRecord(
                id=uuid4().hex,
                event_type=event_type,
                start_file=start_file,
                start_row=start_row,
                end_file=end_file,
                end_row=end_row,
            )
            self._events.append(event)
            self._save_events()
            return event

    def delete_event(self, event_id: str) -> None:
        with self._lock:
            remaining = [event for event in self._events if event.id != event_id]
            if len(remaining) == len(self._events):
                raise KeyError(event_id)
            self._events = remaining
            self._save_events()

    def update_event_type(self, event_id: str, event_type: EventType) -> EventRecord:
        if event_type not in {"overflow", "lost"}:
            raise ValueError("事件类型必须是 overflow 或 lost")

        with self._lock:
            for event in self._events:
                if event.id == event_id:
                    event.event_type = event_type
                    self._save_events()
                    return event

        raise KeyError(event_id)

    # ------------------------------------------------------------------
    # Export logic
    # ------------------------------------------------------------------
    def export_marked_data(self, output_dir: Path | None = None) -> List[str]:
        output_dir = Path(output_dir) if output_dir else self.marked_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        overflow_dir = output_dir / "overflow"
        lost_dir = output_dir / "lost"
        overflow_dir.mkdir(parents=True, exist_ok=True)
        lost_dir.mkdir(parents=True, exist_ok=True)

        exported_files: List[str] = []

        with self._lock:
            if not self._files:
                raise RuntimeError("尚未加载任何CSV文件")

            events_by_file = self._build_file_ranges()

            for info in self._files:
                ranges = events_by_file.get(info.name)
                if not ranges:
                    continue

                # Try multiple encodings to read the CSV file
                df = None
                encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
                for encoding in encodings:
                    try:
                        df = pd.read_csv(info.path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    raise RuntimeError(f"无法读取文件 {info.name}，尝试了多种编码均失败")
                if df.empty:
                    continue

                df["overflow"] = 0
                df["lost"] = 0

                has_annotation = False
                file_has_overflow = False
                file_has_lost = False

                for event in ranges:
                    start_idx = max(event["start_row"] - 1, 0)
                    end_idx = min(event["end_row"] - 1, len(df) - 1)
                    if start_idx > end_idx:
                        continue
                    has_annotation = True
                    column_index = df.columns.get_loc(event["event_type"])
                    df.iloc[start_idx : end_idx + 1, column_index] = 1
                    if event["event_type"] == "overflow":
                        file_has_overflow = True
                    else:
                        file_has_lost = True

                if not has_annotation:
                    continue

                general_path = output_dir / f"{info.name[:-4]}_annotated.csv"
                df.to_csv(general_path, index=False, encoding="utf-8-sig")
                exported_files.append(str(general_path))

                if file_has_overflow:
                    overflow_rows = df[df["overflow"] == 1]
                    if not overflow_rows.empty:
                        overflow_path = overflow_dir / f"{info.name[:-4]}_overflow.csv"
                        overflow_rows.to_csv(overflow_path, index=False, encoding="utf-8-sig")
                        exported_files.append(str(overflow_path))

                if file_has_lost:
                    lost_rows = df[df["lost"] == 1]
                    if not lost_rows.empty:
                        lost_path = lost_dir / f"{info.name[:-4]}_lost.csv"
                        lost_rows.to_csv(lost_path, index=False, encoding="utf-8-sig")
                        exported_files.append(str(lost_path))

        return exported_files

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _validate_event(
        self,
        event_type: EventType,
        start_file: str,
        start_row: int,
        end_file: str,
        end_row: int,
    ) -> None:
        if event_type not in {"overflow", "lost"}:
            raise ValueError("事件类型必须是 overflow 或 lost")

        if start_file not in self._file_index:
            raise ValueError(f"起始文件 {start_file} 不存在")
        if end_file not in self._file_index:
            raise ValueError(f"结束文件 {end_file} 不存在")

        start_info = self._files[self._file_index[start_file]]
        end_info = self._files[self._file_index[end_file]]

        if not (1 <= start_row <= start_info.row_count):
            raise ValueError(f"起始行号应在 1 到 {start_info.row_count} 之间")
        if not (1 <= end_row <= end_info.row_count):
            raise ValueError(f"结束行号应在 1 到 {end_info.row_count} 之间")

        start_pos = self._file_index[start_file]
        end_pos = self._file_index[end_file]

        if start_pos > end_pos or (start_pos == end_pos and start_row > end_row):
            raise ValueError("事件范围必须从早到晚且行号递增")

    def _build_file_ranges(self) -> Dict[str, List[Dict[str, int | str]]]:
        events_map: Dict[str, List[Dict[str, int | str]]] = {}
        for event in self._events:
            start_idx = self._file_index[event.start_file]
            end_idx = self._file_index[event.end_file]

            for file_idx in range(start_idx, end_idx + 1):
                file_info = self._files[file_idx]
                start_row = 1
                end_row = file_info.row_count

                if file_idx == start_idx:
                    start_row = event.start_row
                if file_idx == end_idx:
                    end_row = event.end_row

                events_map.setdefault(file_info.name, []).append(
                    {
                        "event_type": event.event_type,
                        "start_row": start_row,
                        "end_row": end_row,
                    }
                )
        return events_map

    @staticmethod
    def _count_rows(file_path: Path) -> int:
        with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
            total = sum(1 for _ in fh)
        return max(total - 1, 0)
