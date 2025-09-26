from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Literal

EventType = Literal["overflow", "lost"]


@dataclass(slots=True)
class CSVFileInfo:
    """Metadata about a source CSV file."""

    name: str
    path: Path
    row_count: int
    chart_path: Path
    chart_relative_path: str
    encoding: str

    def to_public_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "chart_path": f"/charts/{self.chart_relative_path}",
        }


@dataclass(slots=True)
class EventRecord:
    """Represents a single annotated event."""

    id: str
    event_type: EventType
    start_file: str
    start_row: int
    end_file: str
    end_row: int

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)
