from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pytest

from app.state import AnnotationManager


@pytest.fixture()
def sample_env(tmp_path: Path) -> AnnotationManager:
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()

    data1 = pd.DataFrame({
        "钻头深度(m)": [100, 110, 120],
        "立压log(MPa)": [10, 11, 12],
    })
    data2 = pd.DataFrame({
        "钻头深度(m)": [130, 140, 150],
        "立压log(MPa)": [13, 14, 15],
    })

    data1.to_csv(csv_dir / "Rec1901010000.csv", index=False, encoding="utf-8-sig")
    data2.to_csv(csv_dir / "Rec1901010100.csv", index=False, encoding="utf-8-sig")

    manager = AnnotationManager(
        csv_dir=csv_dir,
        chart_dir=tmp_path / "charts",
        events_file=tmp_path / "marked" / "events.json",
        max_chart_points=50,
        chart_chunk_size=10,
        chart_subdir=None,
    )
    manager.initialize()
    return manager


def test_add_event_and_export(sample_env: AnnotationManager, tmp_path: Path) -> None:
    manager = sample_env

    files = manager.list_files()
    assert [item["name"] for item in files] == ["Rec1901010000.csv", "Rec1901010100.csv"]

    manager.add_event(
        event_type="overflow",
        start_file="Rec1901010000.csv",
        start_row=2,
        end_file="Rec1901010100.csv",
        end_row=2,
    )

    output_dir = tmp_path / "exported"
    exported = manager.export_marked_data(output_dir=output_dir)

    expected_paths = {
        output_dir / "Rec1901010000_annotated.csv",
        output_dir / "Rec1901010100_annotated.csv",
        output_dir / "overflow" / "Rec1901010000_overflow.csv",
        output_dir / "overflow" / "Rec1901010100_overflow.csv",
    }

    assert {Path(p) for p in exported} == expected_paths

    df_first = pd.read_csv(output_dir / "Rec1901010000_annotated.csv", encoding="utf-8-sig")
    df_second = pd.read_csv(output_dir / "Rec1901010100_annotated.csv", encoding="utf-8-sig")

    assert df_first["overflow"].tolist() == [0, 1, 1]
    assert df_second["overflow"].tolist() == [1, 1, 0]

    overflow_first = pd.read_csv(output_dir / "overflow" / "Rec1901010000_overflow.csv", encoding="utf-8-sig")
    overflow_second = pd.read_csv(output_dir / "overflow" / "Rec1901010100_overflow.csv", encoding="utf-8-sig")

    assert overflow_first["overflow"].tolist() == [1, 1]
    assert overflow_second["overflow"].tolist() == [1, 1]


def test_invalid_event_rejected(sample_env: AnnotationManager) -> None:
    with pytest.raises(ValueError):
        sample_env.add_event(
            event_type="lost",
            start_file="Rec1901010000.csv",
            start_row=0,
            end_file="Rec1901010000.csv",
            end_row=1,
        )

    with pytest.raises(ValueError):
        sample_env.add_event(
            event_type="lost",
            start_file="Rec1901010100.csv",
            start_row=2,
            end_file="Rec1901010000.csv",
            end_row=2,
        )


def test_update_event_type(sample_env: AnnotationManager) -> None:
    event = sample_env.add_event(
        event_type="overflow",
        start_file="Rec1901010000.csv",
        start_row=1,
        end_file="Rec1901010000.csv",
        end_row=1,
    )

    updated = sample_env.update_event_type(event.id, "lost")
    assert updated.event_type == "lost"

    all_events = sample_env.list_events()
    assert all_events[0]["event_type"] == "lost"
