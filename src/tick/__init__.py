"""tick: Universal automatic database discovery for data engineering."""

from tick.diff import diff_snapshots, load_snapshot
from tick.engine import discover_database
from tick.models import DiscoveryConfig, DiscoveryResult, SnapshotDiff

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "DiscoveryConfig",
    "DiscoveryResult",
    "SnapshotDiff",
    "discover_database",
    "diff_snapshots",
    "load_snapshot",
]
