"""tick: Universal automatic database discovery for data engineering."""

from tick.diff import diff_snapshots, load_snapshot
from tick.engine import discover_database
from tick.harmonize import harmonize_snapshots
from tick.models import DiscoveryConfig, DiscoveryResult, HarmonizationReport, SnapshotDiff

__version__ = "0.5.0"

__all__ = [
    "__version__",
    "DiscoveryConfig",
    "DiscoveryResult",
    "HarmonizationReport",
    "SnapshotDiff",
    "discover_database",
    "diff_snapshots",
    "harmonize_snapshots",
    "load_snapshot",
]
