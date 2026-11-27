"""Top-level discovery pipeline orchestrator."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone

from tick.catalog import introspect_catalog
from tick.dialects import create_db_engine, get_dialect_adapter, sanitize_url
from tick.models import DatabaseOverview, DiscoveryConfig, DiscoveryResult
from tick.profiler import profile_tables
from tick.recommendations import analyze_tables_and_quality
from tick.relationships import discover_relationships, discover_view_lineage

ProgressCallback = Callable[[str], None]


def discover_database(
    target: str,
    config: DiscoveryConfig | None = None,
    progress_cb: ProgressCallback | None = None,
) -> DiscoveryResult:
    """Connect to any SQLAlchemy-supported database and perform full automatic DE discovery."""
    cfg = config or DiscoveryConfig()
    start = time.perf_counter()

    def _emit(msg: str) -> None:
        if progress_cb:
            progress_cb(msg)

    _emit("Connecting to database...")
    engine, resolved_url = create_db_engine(target, read_only=True)
    adapter = get_dialect_adapter(engine.dialect.name)

    try:
        with engine.connect() as conn:
            server_version = adapter.get_server_version(conn)

        _emit("Introspecting schemas, tables, views, keys, and indexes...")
        schemas, tables, default_schema = introspect_catalog(engine, adapter, cfg)

        _emit(
            f"Profiling {len(tables)} object(s) (mode={cfg.profile_mode}, sample={cfg.sample_size})..."
        )
        value_samples = profile_tables(engine, adapter, tables, default_schema, cfg)

        _emit("Discovering explicit/inferred relationships and view SQL lineage...")
        relationships, fk_lineage = discover_relationships(tables, value_samples, cfg)
        view_lineage = discover_view_lineage(tables, adapter)
        all_lineage = view_lineage + fk_lineage

        _emit("Generating data engineering recommendations and quality scores...")
        quality_issues = analyze_tables_and_quality(tables, relationships)

        duration = round(time.perf_counter() - start, 3)

        base_tables = [t for t in tables if t.table_type == "table"]
        views = [t for t in tables if t.table_type != "table"]
        total_cols = sum(len(t.columns) for t in tables)
        total_rows = sum(t.row_count or 0 for t in base_tables)
        sizes = [t.size_bytes for t in tables if t.size_bytes is not None]
        total_size = sum(sizes) if sizes else None
        pii_cols = sum(
            1
            for t in tables
            for c in t.columns
            if c.profile and c.profile.pii_findings
        )
        explicit_fks = sum(1 for r in relationships if r.kind == "explicit_fk")
        inferred_rels = len(relationships) - explicit_fks
        avg_quality = (
            round(sum(t.quality_score for t in tables) / len(tables), 1)
            if tables
            else 100.0
        )

        overview = DatabaseOverview(
            dialect=engine.dialect.name,
            driver=engine.dialect.driver,
            database_name=adapter.get_database_name(engine),
            server_version=server_version,
            sanitized_url=sanitize_url(resolved_url),
            discovered_at=datetime.now(timezone.utc).isoformat(),
            duration_seconds=duration,
            schema_count=len(schemas),
            table_count=len(base_tables),
            view_count=len(views),
            column_count=total_cols,
            total_rows=total_rows,
            total_size_bytes=total_size,
            pii_column_count=pii_cols,
            explicit_fk_count=explicit_fks,
            inferred_relationship_count=inferred_rels,
            avg_quality_score=avg_quality,
        )

        return DiscoveryResult(
            config=cfg,
            overview=overview,
            schemas=schemas,
            tables=tables,
            relationships=relationships,
            lineage=all_lineage,
            quality_issues=quality_issues,
        )
    finally:
        engine.dispose()
