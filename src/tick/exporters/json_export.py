"""JSON snapshot and diff exporters."""

from __future__ import annotations

from pathlib import Path

from tick.models import DiscoveryResult, SnapshotDiff


def export_json(result: DiscoveryResult, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    return path


def export_diff_json(diff: SnapshotDiff, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(diff.model_dump_json(indent=2), encoding="utf-8")
    return path
