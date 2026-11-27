"""Self-contained interactive HTML discovery report generator (zero external CDNs)."""

from __future__ import annotations

import html
import json
from pathlib import Path

from tick.exporters.databricks_export import (
    render_lakeflow_pipeline_py,
    render_unity_catalog_ddl,
)
from tick.exporters.dbt_export import render_dbt_sources_yaml
from tick.models import DiscoveryResult

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>tick Discovery — __TITLE__</title>
<style>
  :root {
    --bg: #0b0f17;
    --bg-elev: #121826;
    --bg-card: #161e2e;
    --bg-hover: #1e293b;
    --border: #243047;
    --text: #e2e8f0;
    --text-muted: #94a3b8;
    --accent: #38bdf8;
    --accent-soft: rgba(56, 189, 248, 0.14);
    --green: #34d399;
    --green-soft: rgba(52, 211, 153, 0.14);
    --amber: #fbbf24;
    --amber-soft: rgba(251, 191, 36, 0.14);
    --red: #f87171;
    --red-soft: rgba(248, 113, 113, 0.15);
    --purple: #a78bfa;
    --purple-soft: rgba(167, 139, 250, 0.15);
    --orange: #fb923c;
    --orange-soft: rgba(251, 146, 60, 0.15);
    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    --sans: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  [data-theme="light"] {
    --bg: #f8fafc;
    --bg-elev: #ffffff;
    --bg-card: #ffffff;
    --bg-hover: #f1f5f9;
    --border: #e2e8f0;
    --text: #0f172a;
    --text-muted: #64748b;
    --accent: #0284c7;
    --accent-soft: rgba(2, 132, 199, 0.1);
    --green: #059669;
    --green-soft: rgba(5, 150, 105, 0.1);
    --amber: #d97706;
    --amber-soft: rgba(217, 119, 6, 0.12);
    --red: #dc2626;
    --red-soft: rgba(220, 38, 38, 0.1);
    --purple: #7c3aed;
    --purple-soft: rgba(124, 58, 237, 0.1);
    --orange: #ea580c;
    --orange-soft: rgba(234, 88, 12, 0.12);
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: var(--sans); background: var(--bg); color: var(--text);
    line-height: 1.45; font-size: 14px;
  }
  header {
    position: sticky; top: 0; z-index: 40;
    background: var(--bg-elev); border-bottom: 1px solid var(--border);
    padding: 12px 22px; display: flex; align-items: center; justify-content: space-between; gap: 14px;
    flex-wrap: wrap;
  }
  .brand { display: flex; align-items: center; gap: 12px; }
  .logo {
    width: 32px; height: 32px; border-radius: 8px; background: var(--accent-soft);
    border: 1px solid var(--accent); color: var(--accent); font-weight: 800;
    display: grid; place-items: center; font-family: var(--mono); font-size: 15px;
  }
  .brand h1 { margin: 0; font-size: 16px; font-weight: 700; }
  .brand .sub { font-size: 12px; color: var(--text-muted); font-family: var(--mono); }
  .nav { display: flex; gap: 5px; flex-wrap: wrap; }
  .header-actions { display: flex; gap: 6px; align-items: center; }
  .tab-btn {
    background: transparent; color: var(--text-muted); border: 1px solid transparent;
    padding: 7px 12px; border-radius: 7px; cursor: pointer; font-weight: 600; font-size: 13px;
    transition: all 0.15s ease;
  }
  .tab-btn:hover { background: var(--bg-hover); color: var(--text); }
  .tab-btn.active { background: var(--accent-soft); color: var(--accent); border-color: rgba(56,189,248,0.35); }
  .theme-btn {
    background: var(--bg-card); color: var(--text); border: 1px solid var(--border);
    padding: 6px 10px; border-radius: 7px; cursor: pointer; font-size: 12px; font-weight: 600;
  }
  .theme-btn:hover { background: var(--bg-hover); }
  main { padding: 20px 24px; max-width: 1580px; margin: 0 auto; }
  .panel { display: none; }
  .panel.active { display: block; }
  .kpi-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    gap: 14px; margin-bottom: 18px;
  }
  .kpi {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px;
  }
  .kpi .label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted); }
  .kpi .val { font-size: 22px; font-weight: 700; margin-top: 4px; font-family: var(--mono); }
  .kpi .hint { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
  @media (max-width: 1120px) { .grid-3 { grid-template-columns: 1fr; } }
  @media (max-width: 980px) { .grid-2 { grid-template-columns: 1fr; } }
  .card {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px;
    padding: 16px; margin-bottom: 16px;
  }
  .card h2 { margin: 0 0 12px 0; font-size: 15px; font-weight: 700; }
  .card h3 { margin: 0 0 8px 0; font-size: 12px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--text-muted); font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; background: var(--bg-elev); position: sticky; top: 0; }
  tr:hover td { background: var(--bg-hover); }
  .mono { font-family: var(--mono); font-size: 12px; }
  .badge {
    display: inline-block; padding: 2px 7px; border-radius: 999px; font-size: 11px;
    font-weight: 600; font-family: var(--mono); white-space: nowrap; margin-right: 4px; margin-bottom: 2px;
  }
  .b-accent { background: var(--accent-soft); color: var(--accent); }
  .b-green { background: var(--green-soft); color: var(--green); }
  .b-amber { background: var(--amber-soft); color: var(--amber); }
  .b-red { background: var(--red-soft); color: var(--red); }
  .b-purple { background: var(--purple-soft); color: var(--purple); }
  .b-orange { background: var(--orange-soft); color: var(--orange); }
  .b-muted { background: var(--bg-hover); color: var(--text-muted); }

  .donut-wrap { display: flex; align-items: center; gap: 18px; flex-wrap: wrap; }
  .legend-list { display: flex; flex-direction: column; gap: 7px; flex: 1; min-width: 140px; }
  .legend-row { display: flex; align-items: center; justify-content: space-between; font-size: 12.5px; gap: 8px; }
  .legend-swatch { width: 10px; height: 10px; border-radius: 3px; display: inline-block; margin-right: 7px; flex-shrink: 0; }

  .wave-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  @media (max-width: 980px) { .wave-grid { grid-template-columns: 1fr; } }
  .wave-card {
    background: var(--bg); border: 1px solid var(--border); border-radius: 9px; padding: 12px 14px;
    border-top: 3px solid var(--border);
  }
  .wave-card.w1 { border-top-color: var(--green); }
  .wave-card.w2 { border-top-color: var(--amber); }
  .wave-card.w3 { border-top-color: var(--red); }
  .wave-head { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
  .wave-title { font-weight: 700; font-size: 13px; }
  .wave-metrics { font-size: 11.5px; color: var(--text-muted); font-family: var(--mono); margin: 5px 0 8px 0; }
  .wave-pills { display: flex; flex-wrap: wrap; gap: 4px; max-height: 115px; overflow-y: auto; }

  .catalog-layout { display: grid; grid-template-columns: 310px 1fr; gap: 16px; align-items: start; }
  @media (max-width: 900px) { .catalog-layout { grid-template-columns: 1fr; } }
  .sidebar {
    background: var(--bg-card); border: 1px solid var(--border); border-radius: 10px;
    padding: 12px; position: sticky; top: 72px; max-height: calc(100vh - 96px); overflow-y: auto;
  }
  .search-input, .select-input {
    width: 100%; padding: 8px 10px; border-radius: 7px; border: 1px solid var(--border);
    background: var(--bg); color: var(--text); font-size: 13px; margin-bottom: 8px;
  }
  .tbl-item {
    padding: 8px 10px; border-radius: 7px; cursor: pointer; margin-bottom: 4px;
    border: 1px solid transparent; transition: background 0.12s;
  }
  .tbl-item:hover { background: var(--bg-hover); }
  .tbl-item.selected { background: var(--accent-soft); border-color: rgba(56,189,248,0.4); }
  .tbl-item .name { font-weight: 600; font-family: var(--mono); font-size: 12.5px; word-break: break-all; }
  .tbl-item .sem { font-size: 11px; color: var(--accent); }
  .tbl-item .meta { font-size: 11px; color: var(--text-muted); display: flex; justify-content: space-between; margin-top: 3px; }

  .bar-track {
    width: 72px; height: 7px; background: var(--bg); border: 1px solid var(--border);
    border-radius: 99px; overflow: hidden; display: inline-block; vertical-align: middle; margin-right: 6px;
  }
  .bar-fill { height: 100%; border-radius: 99px; }

  .val-bar-row { margin-bottom: 4px; }
  .val-bar-label { display: flex; justify-content: space-between; gap: 6px; font-size: 11px; font-family: var(--mono); }
  .val-bar-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 135px; }
  .val-bar-track { width: 100%; height: 5px; background: var(--bg); border-radius: 99px; overflow: hidden; margin-top: 2px; border: 1px solid var(--border); }
  .val-bar-fill { height: 100%; background: var(--accent); border-radius: 99px; }

  .rec-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; margin: 12px 0; }
  .rec-box { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; }
  .rec-box .k { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; }
  .rec-box .v { margin-top: 4px; font-weight: 600; font-size: 13px; }
  .rec-box .d { margin-top: 3px; font-size: 12px; color: var(--text-muted); }

  pre.code-block {
    background: var(--bg); border: 1px solid var(--border); border-radius: 8px;
    padding: 12px; overflow-x: auto; font-family: var(--mono); font-size: 12px; line-height: 1.5;
    max-height: 520px; overflow-y: auto;
  }

  .canvas-toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }
  .canvas-toolbar .search-input { width: 200px; margin: 0; padding: 5px 9px; font-size: 12px; }
  .btn {
    background: var(--bg-elev); color: var(--text); border: 1px solid var(--border);
    padding: 6px 11px; border-radius: 7px; cursor: pointer; font-size: 12px; font-weight: 600;
  }
  .btn:hover { background: var(--bg-hover); }
  .btn-primary { background: var(--accent-soft); color: var(--accent); border-color: rgba(56,189,248,0.45); }
  .svg-wrap {
    width: 100%; height: 640px; border: 1px solid var(--border); border-radius: 10px;
    background: radial-gradient(circle at 1px 1px, var(--border) 1px, transparent 0);
    background-size: 22px 22px; overflow: hidden; position: relative; cursor: grab;
  }
  .svg-wrap:active { cursor: grabbing; }
  .filters-row { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; align-items: center; }
  .filters-row .search-input, .filters-row .select-input { width: auto; min-width: 190px; margin: 0; }
  .expand-row { font-size: 12px; color: var(--text-muted); margin-top: 4px; }
  .clickable { cursor: pointer; color: var(--accent); text-decoration: underline; text-decoration-style: dotted; }
  .subtabs { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
  .check-pill { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; color: var(--text-muted); cursor: pointer; }

  @media print {
    :root, [data-theme="dark"], [data-theme="light"] {
      --bg: #ffffff; --bg-elev: #f8fafc; --bg-card: #ffffff; --bg-hover: #f1f5f9;
      --border: #cbd5e1; --text: #0f172a; --text-muted: #475569;
    }
    header .nav, header .header-actions, .canvas-toolbar, .filters-row, .sidebar { display: none !important; }
    header { position: static; border-bottom: 2px solid #0f172a; }
    main { padding: 12px 0; max-width: 100%; }
    .panel { display: none !important; }
    #tab-overview, #tab-databricks { display: block !important; page-break-after: always; }
    .card { break-inside: avoid; box-shadow: none; }
    .wave-pills { max-height: none; overflow: visible; }
  }
</style>
</head>
<body>
<header>
  <div class="brand">
    <div class="logo">tk</div>
    <div>
      <h1 id="hdr-title">Database Discovery</h1>
      <div class="sub" id="hdr-sub"></div>
    </div>
  </div>
  <nav class="nav">
    <button class="tab-btn active" data-tab="overview">Overview</button>
    <button class="tab-btn" data-tab="catalog">Catalog &amp; Profiler</button>
    <button class="tab-btn" data-tab="databricks">Databricks &amp; Mfg</button>
    <button class="tab-btn" data-tab="erd">Interactive ERD</button>
    <button class="tab-btn" data-tab="lineage">Lineage Graph</button>
    <button class="tab-btn" data-tab="quality">Quality &amp; Gov</button>
    <button class="tab-btn" data-tab="code">Code &amp; DDL</button>
  </nav>
  <div class="header-actions">
    <button class="theme-btn" id="print-btn" title="Print or Save Executive Summary as PDF">Print / PDF</button>
    <button class="theme-btn" id="theme-toggle">Light / Dark</button>
  </div>
</header>

<main>
  <!-- OVERVIEW -->
  <section id="tab-overview" class="panel active">
    <div class="kpi-grid" id="kpi-grid"></div>

    <div class="grid-3">
      <div class="card">
        <h2>CDC &amp; Lakeflow Readiness</h2>
        <div id="chart-cdc-donut"></div>
      </div>
      <div class="card">
        <h2>Table Health Distribution</h2>
        <div id="chart-health-bars"></div>
      </div>
      <div class="card">
        <h2>Entity Role Distribution</h2>
        <div id="chart-role-bars"></div>
      </div>
    </div>

    <div class="card">
      <h2>Databricks Migration Wave Planner</h2>
      <div class="wave-grid" id="migration-waves"></div>
    </div>

    <div class="grid-2">
      <div class="card">
        <h2>Table Health &amp; Volume</h2>
        <div id="overview-health-list"></div>
      </div>
      <div class="card">
        <h2>Schemas &amp; Entity Roles</h2>
        <div id="overview-schemas"></div>
        <h3 style="margin-top:16px">Entity Classification Breakdown</h3>
        <div id="overview-roles" style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;"></div>
      </div>
    </div>
  </section>

  <!-- CATALOG -->
  <section id="tab-catalog" class="panel">
    <div class="catalog-layout">
      <aside class="sidebar">
        <input id="cat-search" class="search-input" type="text" placeholder="Filter tables, semantic names, cols..." />
        <select id="cat-filter" class="select-input">
          <option value="all">All Objects</option>
          <option value="table">Base Tables Only</option>
          <option value="view">Views Only</option>
          <option value="pii">Contains PII / Governance Tags</option>
          <option value="mfg">Manufacturing Signals (OT/BOM/Dates)</option>
          <option value="issues">Has Quality / Delta Issues</option>
        </select>
        <div id="cat-list"></div>
      </aside>
      <div id="cat-detail"></div>
    </div>
  </section>

  <!-- DATABRICKS & MANUFACTURING -->
  <section id="tab-databricks" class="panel">
    <div class="card" id="cdc-audit-card"></div>
    <div class="card">
      <h2>Databricks Lakeflow Connect, Sizing &amp; Liquid Clustering Blueprint</h2>
      <div style="overflow-x:auto"><table id="dbx-table"></table></div>
    </div>
    <div class="grid-2">
      <div class="card">
        <h2>Manufacturing Signals (OT Telemetry, BOMs, Legacy Dates)</h2>
        <div id="mfg-signals"></div>
      </div>
      <div class="card">
        <h2>Spark / Delta Lake Compatibility Linter</h2>
        <div style="overflow-x:auto"><table id="delta-lint-table"></table></div>
      </div>
    </div>
  </section>

  <!-- ERD -->
  <section id="tab-erd" class="panel">
    <div class="card">
      <div class="canvas-toolbar">
        <strong>Entity-Relationship Diagram</strong>
        <input id="erd-search" class="search-input" type="text" placeholder="Highlight table..." />
        <label class="check-pill">
          <input type="checkbox" id="erd-show-inferred" checked /> Inferred (dashed)
        </label>
        <button class="btn" id="erd-zoom-in">Zoom +</button>
        <button class="btn" id="erd-zoom-out">Zoom -</button>
        <button class="btn" id="erd-fit">Fit View</button>
        <button class="btn" id="erd-reset">Reset</button>
        <button class="btn btn-primary" id="erd-download-svg">Download SVG</button>
        <span style="font-size:12px;color:var(--text-muted)">Scroll to zoom · Drag background to pan · Drag tables · Click title to inspect</span>
      </div>
      <div class="svg-wrap" id="erd-wrap">
        <svg id="erd-svg" width="100%" height="100%"></svg>
      </div>
    </div>
    <div class="card">
      <h2>All Relationships (Explicit &amp; Inferred)</h2>
      <div style="overflow-x:auto"><table id="rel-table"></table></div>
    </div>
  </section>

  <!-- LINEAGE -->
  <section id="tab-lineage" class="panel">
    <div class="card">
      <div class="canvas-toolbar">
        <strong>Data Lineage DAG</strong>
        <input id="lin-search" class="search-input" type="text" placeholder="Highlight node..." />
        <label class="check-pill">
          <input type="checkbox" id="lin-include-fk" /> Include Foreign Key Dependencies
        </label>
        <button class="btn" id="lin-zoom-in">Zoom +</button>
        <button class="btn" id="lin-zoom-out">Zoom -</button>
        <button class="btn" id="lin-fit">Fit View</button>
        <button class="btn" id="lin-reset">Reset</button>
        <button class="btn btn-primary" id="lin-download-svg">Download SVG</button>
        <span style="font-size:12px;color:var(--text-muted)">Tables (blue) &rarr; Views / Materialized Views (purple)</span>
      </div>
      <div class="svg-wrap" id="lin-wrap" style="height:540px;">
        <svg id="lin-svg" width="100%" height="100%"></svg>
      </div>
    </div>
  </section>

  <!-- QUALITY & GOV -->
  <section id="tab-quality" class="panel">
    <div class="card">
      <h2>Data Quality, PII, Unity Catalog Governance &amp; Compatibility Findings</h2>
      <div class="filters-row">
        <input id="q-search" class="search-input" type="text" placeholder="Search table, column, message..." />
        <select id="q-sev" class="select-input">
          <option value="all">All Severities</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>
        <select id="q-cat" class="select-input">
          <option value="all">All Categories</option>
        </select>
      </div>
      <div style="overflow-x:auto"><table id="quality-table"></table></div>
    </div>
  </section>

  <!-- CODE & DDL -->
  <section id="tab-code" class="panel">
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px;">
        <div class="subtabs">
          <button class="btn btn-primary code-tab" data-code="uc_ddl">Unity Catalog Delta DDL</button>
          <button class="btn code-tab" data-code="lakeflow_py">Lakeflow Pipeline (DLT Python)</button>
          <button class="btn code-tab" data-code="dbt_yaml">dbt sources.yml</button>
        </div>
        <div style="display:flex;gap:8px;">
          <button class="btn" id="download-code">Download File</button>
          <button class="btn btn-primary" id="copy-code">Copy Active Code</button>
        </div>
      </div>
      <pre class="code-block" id="code-preview"></pre>
    </div>
  </section>
</main>

<script id="tick-data" type="application/json">__PAYLOAD_JSON__</script>
<script>
(function() {
  const payload = JSON.parse(document.getElementById("tick-data").textContent);
  const data = payload.result;
  const codeAssets = {
    uc_ddl: payload.uc_ddl,
    lakeflow_py: payload.lakeflow_py,
    dbt_yaml: payload.dbt_yaml
  };
  const codeFilenames = {
    uc_ddl: "unity_catalog_ddl.sql",
    lakeflow_py: "lakeflow_pipeline.py",
    dbt_yaml: "sources.yml"
  };
  const ov = data.overview;

  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const fmtInt = (n) => n == null ? "—" : Number(n).toLocaleString();
  const fmtBytes = (b) => {
    if (b == null) return "—";
    const u = ["B","KB","MB","GB","TB"];
    let v = b, i = 0;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return i === 0 ? `${v} B` : `${v.toFixed(1)} ${u[i]}`;
  };
  const fmtMb = (mb) => mb >= 1024 ? `${(mb / 1024).toFixed(2)} GB` : `${mb.toFixed(2)} MB`;
  const healthBadge = (score) => {
    const cls = score >= 85 ? "b-green" : score >= 65 ? "b-amber" : "b-red";
    return `<span class="badge ${cls}">${score}/100</span>`;
  };
  const sevBadge = (sev) => {
    const cls = sev === "high" ? "b-red" : sev === "medium" ? "b-amber" : "b-muted";
    return `<span class="badge ${cls}">${esc(sev.toUpperCase())}</span>`;
  };
  const cdcBadge = (st) => {
    if (!st) return "—";
    const cls = st === "ready" ? "b-green" : st === "blocked" ? "b-red" : st === "needs_config" ? "b-amber" : "b-muted";
    return `<span class="badge ${cls}">${esc(st.toUpperCase())}</span>`;
  };

  function downloadBlob(filename, content, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function exportSvgElement(svgEl, filename) {
    const style = getComputedStyle(document.documentElement);
    const vars = ["--bg", "--bg-elev", "--bg-card", "--border", "--text", "--text-muted", "--accent", "--green", "--amber", "--red", "--purple", "--orange"];
    let raw = svgEl.outerHTML;
    vars.forEach(v => {
      const val = style.getPropertyValue(v).trim();
      if (val) raw = raw.split(`var(${v})`).join(val);
    });
    const bg = style.getPropertyValue("--bg").trim() || "#0b0f17";
    const wrapped = raw.replace("<svg ", `<svg xmlns="http://www.w3.org/2000/svg" style="background:${bg};" `);
    downloadBlob(filename, wrapped, "image/svg+xml;charset=utf-8");
  }

  // Inline SVG Donut Chart
  function renderDonutSvg(segments, centerTop, centerBottom) {
    const total = segments.reduce((acc, s) => acc + s.value, 0);
    const r = 46, cx = 60, cy = 60, c = 2 * Math.PI * r;
    let offset = 0;
    const arcs = total > 0
      ? segments.filter(s => s.value > 0).map(s => {
          const frac = s.value / total;
          const dash = frac * c;
          const arc = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${s.color}" stroke-width="14" stroke-dasharray="${dash.toFixed(2)} ${(c - dash).toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}" transform="rotate(-90 ${cx} ${cy})" />`;
          offset += dash;
          return arc;
        }).join("")
      : `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--border)" stroke-width="14" />`;

    const svg = `<svg width="120" height="120" viewBox="0 0 120 120" style="flex-shrink:0;">
      <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--bg)" stroke-width="14" />
      ${arcs}
      <text x="${cx}" y="${cy - 2}" text-anchor="middle" fill="var(--text)" font-size="17" font-weight="700" font-family="monospace">${esc(centerTop)}</text>
      <text x="${cx}" y="${cy + 14}" text-anchor="middle" fill="var(--text-muted)" font-size="10" font-family="sans-serif">${esc(centerBottom)}</text>
    </svg>`;
    const legend = `<div class="legend-list">${segments.map(s => `
      <div class="legend-row">
        <span><span class="legend-swatch" style="background:${s.color}"></span>${esc(s.label)}</span>
        <span class="mono"><strong>${s.value}</strong>${total > 0 ? ` <span style="color:var(--text-muted)">(${Math.round(s.value / total * 100)}%)</span>` : ""}</span>
      </div>`).join("")}</div>`;
    return `<div class="donut-wrap">${svg}${legend}</div>`;
  }

  // Inline SVG Horizontal Bar Chart
  function renderHorizBars(items) {
    if (!items.length) return `<div style="color:var(--text-muted);font-size:12px;">No data available.</div>`;
    const maxVal = Math.max(1, ...items.map(i => i.value));
    return items.map(item => {
      const pct = Math.max(3, Math.round((item.value / maxVal) * 100));
      return `<div style="margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px;">
          <span class="mono">${esc(item.label)}</span>
          <span class="mono"><strong>${item.value}</strong></span>
        </div>
        <div style="width:100%;height:8px;background:var(--bg);border:1px solid var(--border);border-radius:99px;overflow:hidden;">
          <div style="width:${pct}%;height:100%;background:${item.color};border-radius:99px;"></div>
        </div>
      </div>`;
    }).join("");
  }

  // Inline SVG Numeric Quantile Boxplot
  function renderBoxplotSvg(p) {
    const minV = Number(p.min_value), maxV = Number(p.max_value);
    if (!Number.isFinite(minV) || !Number.isFinite(maxV) || p.p25 == null || p.p50 == null || p.p75 == null) return "";
    const span = maxV - minV;
    if (span <= 0) return "";
    const w = 136, h = 24, pad = 8;
    const xOf = (v) => pad + ((Math.min(maxV, Math.max(minV, Number(v))) - minV) / span) * (w - pad * 2);
    const xMin = xOf(minV), x25 = xOf(p.p25), x50 = xOf(p.p50), x75 = xOf(p.p75), xMax = xOf(maxV);
    const meanMarker = p.mean != null && Number.isFinite(Number(p.mean))
      ? `<polygon points="${xOf(p.mean)},5 ${xOf(p.mean)+3.5},12 ${xOf(p.mean)},19 ${xOf(p.mean)-3.5},12" fill="var(--amber)" opacity="0.9"><title>mean: ${p.mean}</title></polygon>`
      : "";
    return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="display:block;margin:4px 0 2px 0;">
      <line x1="${xMin}" y1="12" x2="${xMax}" y2="12" stroke="var(--text-muted)" stroke-width="1.5" />
      <line x1="${xMin}" y1="7" x2="${xMin}" y2="17" stroke="var(--text-muted)" stroke-width="1.5" />
      <line x1="${xMax}" y1="7" x2="${xMax}" y2="17" stroke="var(--text-muted)" stroke-width="1.5" />
      <rect x="${x25}" y="6" width="${Math.max(2, x75 - x25)}" height="12" rx="2" fill="var(--accent-soft)" stroke="var(--accent)" stroke-width="1.4" />
      <line x1="${x50}" y1="5" x2="${x50}" y2="19" stroke="var(--green)" stroke-width="2" />
      ${meanMarker}
    </svg>`;
  }

  // Theme init + persistence
  try {
    const savedTheme = localStorage.getItem("tick-theme");
    if (savedTheme === "light" || savedTheme === "dark") {
      document.documentElement.setAttribute("data-theme", savedTheme);
    }
  } catch (_) {}

  document.getElementById("hdr-title").textContent = `tick · ${ov.database_name}`;
  document.getElementById("hdr-sub").textContent =
    `${ov.dialect} (${ov.server_version || ov.driver}) · ${ov.sanitized_url} · ${ov.duration_seconds}s`;

  document.getElementById("theme-toggle").addEventListener("click", () => {
    const cur = document.documentElement.getAttribute("data-theme");
    const next = cur === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", next);
    try { localStorage.setItem("tick-theme", next); } catch (_) {}
  });
  document.getElementById("print-btn").addEventListener("click", () => window.print());

  const switchTab = (tabId) => {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.toggle("active", b.dataset.tab === tabId));
    document.querySelectorAll(".panel").forEach(p => p.classList.toggle("active", p.id === `tab-${tabId}`));
    if (tabId === "erd") renderERD();
    if (tabId === "lineage") renderLineage();
  };
  document.querySelectorAll(".tab-btn").forEach(b => b.addEventListener("click", () => switchTab(b.dataset.tab)));

  const jumpToTable = (fullName) => {
    selectedTable = fullName;
    switchTab("catalog");
    renderCatalogList();
    renderTableDetail(fullName);
  };
  document.addEventListener("click", (e) => {
    const el = e.target.closest("[data-jump]");
    if (el) jumpToTable(el.getAttribute("data-jump"));
  });

  // OVERVIEW KPIs & CHARTS
  const baseTables = data.tables.filter(t => t.table_type === "table");
  const totalEstMb = baseTables.reduce((acc, t) => acc + (t.ingestion_sizing?.estimated_size_mb || 0), 0);
  const kpis = [
    { label: "Base Tables", val: fmtInt(ov.table_count), hint: `${ov.view_count} views · ${ov.empty_table_count} empty` },
    { label: "Total Rows", val: fmtInt(ov.total_rows), hint: `${fmtInt(ov.column_count)} cols · est. ${fmtMb(totalEstMb)}` },
    { label: "Relationships", val: fmtInt(ov.explicit_fk_count + ov.inferred_relationship_count), hint: `${ov.explicit_fk_count} explicit · ${ov.inferred_relationship_count} inferred` },
    { label: "CDC Readiness", val: `${ov.cdc_ready_table_count} / ${ov.table_count}`, hint: `${ov.cdc_blocked_table_count} blocked · ${ov.delta_warning_count} Delta warns` },
    { label: "PII & UC Gov", val: `${ov.pii_column_count} / ${ov.governance_column_count}`, hint: "PII cols / UC policy cols" },
    { label: "Avg Health", val: `${ov.avg_quality_score}/100`, hint: `${data.quality_issues.length} total findings` },
  ];
  document.getElementById("kpi-grid").innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="label">${k.label}</div><div class="val">${k.val}</div><div class="hint">${k.hint}</div></div>`
  ).join("");

  // CDC Donut Chart
  const cdcReady = baseTables.filter(t => t.cdc_readiness?.status === "ready").length;
  const cdcNeeds = baseTables.filter(t => t.cdc_readiness?.status === "needs_config").length;
  const cdcBlocked = baseTables.filter(t => t.cdc_readiness?.status === "blocked").length;
  const cdcOther = Math.max(0, baseTables.length - cdcReady - cdcNeeds - cdcBlocked);
  const readyPct = baseTables.length ? `${Math.round((cdcReady / baseTables.length) * 100)}%` : "—";
  document.getElementById("chart-cdc-donut").innerHTML = renderDonutSvg(
    [
      { label: "Ready", value: cdcReady, color: "var(--green)" },
      { label: "Needs Config", value: cdcNeeds, color: "var(--amber)" },
      { label: "Blocked", value: cdcBlocked, color: "var(--red)" },
      ...(cdcOther ? [{ label: "Unaudited / N/A", value: cdcOther, color: "var(--text-muted)" }] : [])
    ],
    readyPct,
    "CDC Ready"
  );

  // Health Distribution Bars
  const hGood = data.tables.filter(t => t.quality_score >= 85).length;
  const hFair = data.tables.filter(t => t.quality_score >= 65 && t.quality_score < 85).length;
  const hPoor = data.tables.filter(t => t.quality_score < 65).length;
  document.getElementById("chart-health-bars").innerHTML = renderHorizBars([
    { label: "Healthy (85-100)", value: hGood, color: "var(--green)" },
    { label: "Fair (65-84)", value: hFair, color: "var(--amber)" },
    { label: "Needs Attention (<65)", value: hPoor, color: "var(--red)" },
  ]);

  // Entity Role Bars
  const roleCounts = {};
  data.tables.forEach(t => {
    const r = t.recommendations?.entity_role || "unknown";
    roleCounts[r] = (roleCounts[r] || 0) + 1;
  });
  const rolePalette = ["var(--accent)", "var(--purple)", "var(--green)", "var(--orange)", "var(--amber)", "var(--text-muted)"];
  const roleEntries = Object.entries(roleCounts).sort((a,b) => b[1] - a[1]);
  document.getElementById("chart-role-bars").innerHTML = renderHorizBars(
    roleEntries.slice(0, 5).map(([r, c], idx) => ({ label: r, value: c, color: rolePalette[idx % rolePalette.length] }))
  );

  // Migration Wave Planner
  const wave1 = [], wave2 = [], wave3 = [];
  baseTables.forEach(t => {
    const st = t.cdc_readiness?.status;
    const hasHighDelta = (t.delta_compatibility?.issues || []).some(i => i.severity === "high");
    if (st === "blocked" || hasHighDelta) wave3.push(t);
    else if (st === "ready") wave1.push(t);
    else wave2.push(t);
  });
  function renderWaveCard(cls, title, subtitle, list) {
    const mb = list.reduce((acc, t) => acc + (t.ingestion_sizing?.estimated_size_mb || 0), 0);
    const daily = list.reduce((acc, t) => acc + (t.ingestion_sizing?.estimated_daily_rows || 0), 0);
    return `<div class="wave-card ${cls}">
      <div class="wave-head">
        <span class="wave-title">${title}</span>
        <span class="badge b-muted">${list.length} tables</span>
      </div>
      <div class="wave-metrics">${subtitle} · est. ${fmtMb(mb)}${daily > 0 ? ` · ~${Math.round(daily).toLocaleString()} rows/day` : ""}</div>
      <div class="wave-pills">
        ${list.length ? list.map(t => `<span class="badge b-accent clickable" data-jump="${esc(t.full_name)}">${esc(t.name)}</span>`).join("") : `<span style="color:var(--text-muted);font-size:12px;">No tables in this wave</span>`}
      </div>
    </div>`;
  }
  document.getElementById("migration-waves").innerHTML =
    renderWaveCard("w1", "Wave 1 · Fast-Track Lakeflow", "CDC / Watermark Ready", wave1) +
    renderWaveCard("w2", "Wave 2 · Needs Config", "Enable CT/CDC or PK", wave2) +
    renderWaveCard("w3", "Wave 3 · Remediation Required", "Blocked CDC or High Delta Lint", wave3);

  const sortedTables = [...data.tables].sort((a,b) => (b.row_count || 0) - (a.row_count || 0));
  document.getElementById("overview-health-list").innerHTML = `
    <table>
      <thead><tr><th>Object</th><th>Role</th><th>Rows</th><th>CDC</th><th>Health</th></tr></thead>
      <tbody>
        ${sortedTables.map(t => `
          <tr>
            <td>
              <span class="clickable mono" data-jump="${esc(t.full_name)}">${esc(t.full_name)}</span>
              ${t.semantic_name ? `<div style="font-size:11px;color:var(--accent)">${esc(t.semantic_name)}</div>` : ""}
            </td>
            <td><span class="badge b-purple">${esc(t.recommendations?.entity_role || 'unknown')}</span></td>
            <td class="mono">${fmtInt(t.row_count)}</td>
            <td>${cdcBadge(t.cdc_readiness?.status)}</td>
            <td>${healthBadge(t.quality_score)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>`;

  document.getElementById("overview-schemas").innerHTML = `
    <table>
      <thead><tr><th>Schema</th><th>Tables</th><th>Views</th><th>Rows</th><th>Size</th></tr></thead>
      <tbody>
        ${data.schemas.map(s => `
          <tr>
            <td class="mono"><strong>${esc(s.name)}</strong></td>
            <td>${s.table_count}</td>
            <td>${s.view_count}</td>
            <td class="mono">${fmtInt(s.total_rows)}</td>
            <td class="mono">${fmtBytes(s.total_size_bytes)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>`;

  document.getElementById("overview-roles").innerHTML = roleEntries
    .map(([r, c]) => `<span class="badge b-purple" style="font-size:12px;padding:4px 10px;">${esc(r)}: ${c}</span>`).join("");

  // DATABRICKS & MANUFACTURING TAB
  const audit = ov.cdc_audit;
  document.getElementById("cdc-audit-card").innerHTML = `
    <h2>Source CDC &amp; Lakeflow Connect Readiness (${esc(ov.dialect)})</h2>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;">
      <span class="badge b-green">CDC Ready: ${ov.cdc_ready_table_count}</span>
      <span class="badge b-red">CDC Blocked: ${ov.cdc_blocked_table_count}</span>
      <span class="badge b-amber">Delta Lint Warnings: ${ov.delta_warning_count}</span>
      ${audit?.wal_level ? `<span class="badge b-accent">wal_level: ${esc(audit.wal_level)}</span>` : ""}
      ${audit?.db_change_tracking_enabled != null ? `<span class="badge b-accent">MSSQL CT: ${audit.db_change_tracking_enabled}</span>` : ""}
    </div>
    ${audit?.notes?.length ? `<ul style="margin:6px 0 0 18px;color:var(--text-muted);">${audit.notes.map(n => `<li>${esc(n)}</li>`).join("")}</ul>` : ""}
  `;

  document.getElementById("dbx-table").innerHTML = `
    <thead><tr><th>Table</th><th>Rows / Est. MB</th><th>Daily Rows</th><th>CDC</th><th>Connector</th><th>Liquid Clustering</th><th>Cadence / Tier</th></tr></thead>
    <tbody>
      ${baseTables.map(t => {
        const sz = t.ingestion_sizing || {};
        const cdc = t.cdc_readiness || {};
        const dc = t.delta_compatibility || {};
        return `
          <tr>
            <td>
              <span class="clickable mono" data-jump="${esc(t.full_name)}">${esc(t.full_name)}</span>
              ${t.semantic_name ? `<div style="font-size:11px;color:var(--accent)">${esc(t.semantic_name)}</div>` : ""}
            </td>
            <td class="mono">${fmtInt(t.row_count)}<br><span style="color:var(--text-muted)">${sz.estimated_size_mb != null ? sz.estimated_size_mb.toFixed(2) + ' MB' : '—'}</span></td>
            <td class="mono">${sz.estimated_daily_rows != null ? Math.round(sz.estimated_daily_rows).toLocaleString() : '—'}</td>
            <td>${cdcBadge(cdc.status)}</td>
            <td class="mono">${esc(cdc.connector_recommendation || '—')}</td>
            <td class="mono">${esc((dc.liquid_clustering_keys || []).join(", ")) || "—"}</td>
            <td><span class="badge b-accent">${esc(sz.recommended_cadence || '—')}</span><div style="font-size:11px;color:var(--text-muted)">${esc(sz.recommended_databricks_tier || '')}</div></td>
          </tr>`;
      }).join("")}
    </tbody>`;

  const mfgItems = [];
  data.tables.forEach(t => {
    if (t.telemetry_profile) {
      const tp = t.telemetry_profile;
      mfgItems.push(`<div class="rec-box" style="margin-bottom:8px;">
        <div class="k">OT Telemetry · <span class="clickable" data-jump="${esc(t.full_name)}">${esc(t.full_name)}</span></div>
        <div class="v"><span class="badge b-orange">${esc(tp.pattern)}</span> ts: <code>${esc(tp.timestamp_column)}</code> · cadence: ${tp.median_interval_seconds ?? '—'}s</div>
        <div class="d">Watermark delay: <code>${esc(tp.recommended_watermark_delay)}</code> · Cluster: <code>${esc((tp.recommended_liquid_cluster || []).join(', '))}</code>${tp.flatline_columns?.length ? ` · <span class="badge b-red">Flatlines: ${esc(tp.flatline_columns.join(', '))}</span>` : ""}</div>
      </div>`);
    }
    if (t.hierarchy) {
      mfgItems.push(`<div class="rec-box" style="margin-bottom:8px;">
        <div class="k">Recursive Hierarchy · <span class="clickable" data-jump="${esc(t.full_name)}">${esc(t.full_name)}</span></div>
        <div class="v"><span class="badge b-purple">${esc(t.hierarchy.pattern)}</span> <code>${esc(t.hierarchy.parent_column)} → ${esc(t.hierarchy.child_column)}</code></div>
        <div class="d">${esc(t.hierarchy.reason)}</div>
      </div>`);
    }
    t.columns.forEach(c => {
      if (c.profile?.legacy_date) {
        const ld = c.profile.legacy_date;
        mfgItems.push(`<div class="rec-box" style="margin-bottom:8px;">
          <div class="k">Legacy Date/Time · <span class="clickable" data-jump="${esc(t.full_name)}">${esc(t.full_name)}</span>.${esc(c.name)}</div>
          <div class="v"><span class="badge b-amber">${esc(ld.encoding)}</span>${ld.paired_column ? ` paired with <code>${esc(ld.paired_column)}</code>` : ""}</div>
          <div class="d mono">${esc(ld.spark_expression)}</div>
        </div>`);
      }
    });
  });
  document.getElementById("mfg-signals").innerHTML = mfgItems.length ? mfgItems.join("") : `<div style="color:var(--text-muted)">No OT telemetry, recursive BOMs, or legacy integer dates detected.</div>`;

  const lintRows = [];
  data.tables.forEach(t => {
    (t.delta_compatibility?.issues || []).forEach(iss => lintRows.push({ table: t.full_name, ...iss }));
  });
  document.getElementById("delta-lint-table").innerHTML = `
    <thead><tr><th>Sev</th><th>Table / Col</th><th>Category</th><th>Remediation</th></tr></thead>
    <tbody>
      ${lintRows.length ? lintRows.map(r => `
        <tr>
          <td>${sevBadge(r.severity)}</td>
          <td class="mono"><span class="clickable" data-jump="${esc(r.table)}">${esc(r.table)}</span>${r.column ? '.' + esc(r.column) : ''}</td>
          <td class="mono">${esc(r.category)}</td>
          <td>${esc(r.message)}<div style="font-size:11px;color:var(--accent)">${esc(r.remediation)}</div></td>
        </tr>`).join("") : `<tr><td colspan="4" style="color:var(--text-muted)">All tables cleanly compatible with Spark/Delta Lake.</td></tr>`}
    </tbody>`;

  // CATALOG & PROFILER
  let selectedTable = data.tables[0]?.full_name || null;

  function renderCatalogList() {
    const q = document.getElementById("cat-search").value.toLowerCase().trim();
    const flt = document.getElementById("cat-filter").value;
    const filtered = data.tables.filter(t => {
      if (flt === "table" && t.table_type !== "table") return false;
      if (flt === "view" && t.table_type === "table") return false;
      if (flt === "pii" && !t.columns.some(c => c.profile?.pii_findings?.length || c.profile?.governance_findings?.length)) return false;
      if (flt === "mfg" && !(t.telemetry_profile || t.hierarchy || t.columns.some(c => c.profile?.legacy_date))) return false;
      if (flt === "issues" && t.quality_issues.length === 0) return false;
      if (q) {
        const hitTbl = t.full_name.toLowerCase().includes(q) || (t.semantic_name || "").toLowerCase().includes(q);
        const hitCol = t.columns.some(c => c.name.toLowerCase().includes(q) || (c.semantic_name || "").toLowerCase().includes(q));
        if (!hitTbl && !hitCol) return false;
      }
      return true;
    });

    const el = document.getElementById("cat-list");
    if (!filtered.length) {
      el.innerHTML = `<div style="color:var(--text-muted);padding:12px;">No matching objects</div>`;
      return;
    }
    el.innerHTML = filtered.map(t => {
      const piiN = t.columns.filter(c => c.profile?.pii_findings?.length).length;
      const govN = t.columns.filter(c => c.profile?.governance_findings?.length).length;
      return `
        <div class="tbl-item ${t.full_name === selectedTable ? 'selected' : ''}" data-jump="${esc(t.full_name)}">
          <div class="name">${esc(t.name)} ${t.table_type !== 'table' ? '<span class="badge b-purple">VIEW</span>' : ''}</div>
          ${t.semantic_name ? `<div class="sem">${esc(t.semantic_name)}</div>` : ""}
          <div class="meta">
            <span>${esc(t.schema_name)} · ${fmtInt(t.row_count)} rows</span>
            <span>${piiN ? `<span class="badge b-red">PII:${piiN}</span>` : ''}${govN ? `<span class="badge b-orange">UC:${govN}</span>` : ''}${t.quality_score}/100</span>
          </div>
        </div>
      `;
    }).join("");
  }

  function renderColumnRows(tbl) {
    const q = (document.getElementById("col-search")?.value || "").toLowerCase().trim();
    const piiOnly = document.getElementById("col-pii-only")?.checked || false;
    const highNullOnly = document.getElementById("col-high-null")?.checked || false;

    const cols = tbl.columns.filter(c => {
      const p = c.profile;
      if (piiOnly && !(p?.pii_findings?.length || p?.governance_findings?.length)) return false;
      if (highNullOnly && !(p && !p.profile_error && p.null_rate >= 0.10)) return false;
      if (q && !`${c.name} ${c.semantic_name || ""} ${c.raw_type} ${c.spark_type || ""}`.toLowerCase().includes(q)) return false;
      return true;
    });

    const tbody = document.getElementById("col-tbody");
    if (!tbody) return;
    if (!cols.length) {
      tbody.innerHTML = `<tr><td colspan="7" style="color:var(--text-muted)">No columns match the current filter.</td></tr>`;
      return;
    }

    tbody.innerHTML = cols.map(c => {
      const p = c.profile;
      const keys = [
        c.is_primary_key ? '<span class="badge b-green">PK</span>' : '',
        c.is_foreign_key ? '<span class="badge b-accent">FK</span>' : '',
        (c.is_unique && !c.is_primary_key) ? '<span class="badge b-purple">UQ</span>' : '',
        !c.nullable ? '<span class="badge b-muted">NN</span>' : ''
      ].join("");

      let nullHtml = "—", distHtml = "—", statsHtml = "—", flagsHtml = "—", valsHtml = "—";
      if (p && !p.profile_error) {
        const pct = Math.round(p.null_rate * 100);
        const barColor = pct >= 50 ? "var(--red)" : pct >= 10 ? "var(--amber)" : "var(--green)";
        nullHtml = `<div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${barColor}"></div></div><span class="mono">${(p.null_rate*100).toFixed(1)}%</span>`;
        distHtml = `<div class="mono">${fmtInt(p.distinct_count)}</div><span class="badge b-muted">${esc(p.cardinality_class)}</span>`;

        const st = [];
        if (p.min_value != null) st.push(`min: <code>${esc(p.min_value)}</code>`);
        if (p.max_value != null) st.push(`max: <code>${esc(p.max_value)}</code>`);
        if (p.mean != null) st.push(`mean: <code>${p.mean}</code> · p50: <code>${p.p50}</code>`);
        if (p.avg_length != null) st.push(`len: ${p.min_length}..${p.max_length}`);
        const boxSvg = renderBoxplotSvg(p);
        statsHtml = (st.length || boxSvg) ? `<div class="expand-row" style="margin:0">${boxSvg}${st.join("<br>")}</div>` : "—";

        const flags = [];
        (p.pii_findings || []).forEach(f => {
          flags.push(`<span class="badge b-red">PII: ${esc(f.category)}</span>`);
        });
        (p.governance_findings || []).forEach(g => {
          flags.push(`<span class="badge b-orange">UC ${esc(g.uc_policy)}: ${esc(g.uc_tag_value)}</span>`);
        });
        if (p.legacy_date) {
          flags.push(`<span class="badge b-amber">LEGACY: ${esc(p.legacy_date.encoding)}</span>`);
        }
        if (p.freshness_age_hours != null) {
          const days = (p.freshness_age_hours / 24).toFixed(1);
          flags.push(`<span class="badge ${p.is_stale ? 'b-red' : 'b-green'}">${p.is_stale ? 'STALE' : 'FRESH'}: ${days}d</span>`);
        }
        flagsHtml = flags.join("<br>") || "—";

        if (p.top_values?.length) {
          valsHtml = p.top_values.slice(0,3).map(v => {
            const wPct = Math.max(2, Math.round(v.rate * 100));
            return `<div class="val-bar-row">
              <div class="val-bar-label"><span class="val-bar-name" title="${esc(v.value)}">${esc(v.value)}</span><span style="color:var(--text-muted)">${Math.round(v.rate*100)}%</span></div>
              <div class="val-bar-track"><div class="val-bar-fill" style="width:${wPct}%"></div></div>
            </div>`;
          }).join("");
        }
      } else if (p?.profile_error) {
        nullHtml = `<span class="badge b-red">Error</span>`;
      }

      return `
        <tr>
          <td>
            <div class="mono"><strong>${esc(c.name)}</strong></div>
            ${c.semantic_name ? `<div style="font-size:11px;color:var(--accent)">${esc(c.semantic_name)}</div>` : ""}
            <div style="margin-top:3px;">${keys}</div>
          </td>
          <td><div class="mono">${esc(c.raw_type)}</div>${c.spark_type ? `<span class="badge b-accent">${esc(c.spark_type)}</span>` : ""}</td>
          <td>${nullHtml}</td>
          <td>${distHtml}</td>
          <td>${statsHtml}</td>
          <td>${flagsHtml}</td>
          <td style="min-width:165px;">${valsHtml}</td>
        </tr>`;
    }).join("");
  }

  function renderTableDetail(fullName) {
    const tbl = data.tables.find(t => t.full_name === fullName);
    const box = document.getElementById("cat-detail");
    if (!tbl) {
      box.innerHTML = `<div class="card">Select a table from the sidebar.</div>`;
      return;
    }
    const rec = tbl.recommendations;
    const cdc = tbl.cdc_readiness;
    const dc = tbl.delta_compatibility;
    const sz = tbl.ingestion_sizing;
    const rels = data.relationships.filter(r => r.source_table === tbl.full_name || r.target_table === tbl.full_name);

    box.innerHTML = `
      <div class="card">
        <h2 style="margin:0;font-family:var(--mono);font-size:18px;">
          ${esc(tbl.full_name)} ${tbl.semantic_name ? `<span style="color:var(--accent);font-weight:500;">→ ${esc(tbl.semantic_name)}</span>` : ""}
        </h2>
        <div style="margin-top:6px;">
          <span class="badge b-accent">${esc(tbl.table_type.toUpperCase())}</span>
          ${rec ? `<span class="badge b-purple">role: ${esc(rec.entity_role)}</span>` : ""}
          ${healthBadge(tbl.quality_score)}
          ${cdcBadge(cdc?.status)}
          <span class="badge b-muted">${fmtInt(tbl.row_count)} rows${tbl.row_count_exact ? '' : ' (est)'}</span>
          ${tbl.primary_key.length ? `<span class="badge b-green">PK: ${esc(tbl.primary_key.join(', '))}</span>` : `<span class="badge b-red">NO PK</span>`}
          ${tbl.telemetry_profile ? `<span class="badge b-orange">OT: ${esc(tbl.telemetry_profile.pattern)}</span>` : ""}
          ${tbl.hierarchy ? `<span class="badge b-purple">HIERARCHY: ${esc(tbl.hierarchy.pattern)}</span>` : ""}
        </div>
        ${(tbl.business_description || tbl.comment) ? `<p style="color:var(--text-muted);margin:8px 0 0 0;">${esc(tbl.business_description || tbl.comment)}</p>` : ""}
        ${tbl.skipped_profiling_reason ? `<p style="color:var(--amber);margin:8px 0 0 0;">⚠ ${esc(tbl.skipped_profiling_reason)}</p>` : ""}

        <div class="rec-grid">
          ${rec ? `
          <div class="rec-box">
            <div class="k">Ingestion &amp; CDC</div>
            <div class="v"><span class="badge b-accent">${esc(rec.ingestion_strategy)}</span></div>
            <div class="d">${esc(cdc?.connector_recommendation || '')} · ${esc(rec.ingestion_strategy_reason)}</div>
          </div>` : ""}
          ${dc ? `
          <div class="rec-box">
            <div class="k">Databricks Liquid Clustering</div>
            <div class="v mono">${dc.liquid_clustering_keys?.length ? `CLUSTER BY (${esc(dc.liquid_clustering_keys.join(', '))})` : "None suggested"}</div>
            <div class="d">${dc.requires_column_mapping ? "⚠ Requires delta.columnMapping.mode='name' or column rename" : "Delta column names compliant"}</div>
          </div>` : ""}
          ${sz ? `
          <div class="rec-box">
            <div class="k">Backfill &amp; Cadence Sizing</div>
            <div class="v mono">${sz.estimated_size_mb.toFixed(2)} MB · ${esc(sz.recommended_cadence)}</div>
            <div class="d">${sz.estimated_daily_rows != null ? `~${Math.round(sz.estimated_daily_rows).toLocaleString()} rows/day · ` : ""}${esc(sz.recommended_databricks_tier)}</div>
          </div>` : ""}
          ${rec?.watermark_candidates?.length ? `
          <div class="rec-box">
            <div class="k">Watermark Candidates</div>
            <div class="v mono">${rec.watermark_candidates.map(w => `${esc(w.column)} (${esc(w.kind)})`).join("<br>")}</div>
          </div>` : ""}
        </div>

        ${tbl.upstream_tables.length ? `
          <div style="margin-top:8px;">
            <strong style="font-size:12px;color:var(--text-muted);">UPSTREAM DEPENDENCIES: </strong>
            ${tbl.upstream_tables.map(u => `<span class="badge b-purple clickable" data-jump="${esc(u)}">${esc(u)}</span>`).join("")}
          </div>` : ""}

        ${rels.length ? `
          <div style="margin-top:10px;">
            <strong style="font-size:12px;color:var(--text-muted);">RELATIONSHIPS (${rels.length}): </strong>
            ${rels.map(r => {
              const isOut = r.source_table === tbl.full_name;
              const other = isOut ? r.target_table : r.source_table;
              const cls = r.kind === "explicit_fk" ? "b-green" : "b-amber";
              return `<span class="badge ${cls} clickable" data-jump="${esc(other)}">${isOut ? '→' : '←'} ${esc(other)} (${esc(r.source_column)}→${esc(r.target_column)}${r.kind !== 'explicit_fk' ? ' ' + Math.round(r.confidence*100) + '%' : ''})</span>`;
            }).join("")}
          </div>` : ""}
      </div>

      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:10px;">
          <h2 style="margin:0;">Columns (${tbl.columns.length})</h2>
          <div class="filters-row" style="margin:0;">
            <input id="col-search" class="search-input" type="text" placeholder="Filter columns..." style="min-width:170px;" />
            <label class="check-pill"><input type="checkbox" id="col-pii-only" /> PII / UC Only</label>
            <label class="check-pill"><input type="checkbox" id="col-high-null" /> Nulls ≥ 10%</label>
          </div>
        </div>
        <div style="overflow-x:auto">
          <table>
            <thead>
              <tr>
                <th>Column</th><th>Source → Spark Type</th><th>Null %</th><th>Distinct</th>
                <th>Range / Quantiles</th><th>PII / UC Governance / Signals</th><th>Top Values</th>
              </tr>
            </thead>
            <tbody id="col-tbody"></tbody>
          </table>
        </div>
      </div>

      ${tbl.view_definition ? `<div class="card"><h2>View SQL Definition</h2><pre class="code-block">${esc(tbl.view_definition)}</pre></div>` : ""}
    `;

    ["col-search", "col-pii-only", "col-high-null"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener("input", () => renderColumnRows(tbl));
      if (el && el.type === "checkbox") el.addEventListener("change", () => renderColumnRows(tbl));
    });
    renderColumnRows(tbl);
  }

  document.getElementById("cat-search").addEventListener("input", renderCatalogList);
  document.getElementById("cat-filter").addEventListener("change", renderCatalogList);
  renderCatalogList();
  if (selectedTable) renderTableDetail(selectedTable);

  // QUALITY TAB
  const cats = [...new Set(data.quality_issues.map(i => i.category))].sort();
  const qCatSel = document.getElementById("q-cat");
  cats.forEach(c => {
    const opt = document.createElement("option");
    opt.value = c; opt.textContent = c;
    qCatSel.appendChild(opt);
  });

  function renderQuality() {
    const q = document.getElementById("q-search").value.toLowerCase().trim();
    const sev = document.getElementById("q-sev").value;
    const cat = document.getElementById("q-cat").value;
    const items = data.quality_issues.filter(i => {
      if (sev !== "all" && i.severity !== sev) return false;
      if (cat !== "all" && i.category !== cat) return false;
      if (q && !`${i.table} ${i.column || ''} ${i.category} ${i.message}`.toLowerCase().includes(q)) return false;
      return true;
    });
    document.getElementById("quality-table").innerHTML = `
      <thead><tr><th>Severity</th><th>Category</th><th>Table</th><th>Column</th><th>Details</th></tr></thead>
      <tbody>
        ${items.length ? items.map(i => `
          <tr>
            <td>${sevBadge(i.severity)}</td>
            <td class="mono">${esc(i.category)}</td>
            <td><span class="clickable mono" data-jump="${esc(i.table)}">${esc(i.table)}</span></td>
            <td class="mono">${esc(i.column || '—')}</td>
            <td>${esc(i.message)}</td>
          </tr>`).join("") : `<tr><td colspan="5" style="color:var(--text-muted)">No matching findings.</td></tr>`}
      </tbody>`;
  }
  ["q-search","q-sev","q-cat"].forEach(id => document.getElementById(id).addEventListener("input", renderQuality));
  renderQuality();

  // CODE & DDL TAB
  let activeCodeKey = "uc_ddl";
  function renderCodePreview() {
    document.getElementById("code-preview").textContent = codeAssets[activeCodeKey] || "";
    document.querySelectorAll(".code-tab").forEach(b => {
      b.classList.toggle("btn-primary", b.dataset.code === activeCodeKey);
    });
  }
  document.querySelectorAll(".code-tab").forEach(b => b.addEventListener("click", () => {
    activeCodeKey = b.dataset.code;
    renderCodePreview();
  }));
  document.getElementById("copy-code").addEventListener("click", async (e) => {
    try {
      await navigator.clipboard.writeText(codeAssets[activeCodeKey] || "");
      e.target.textContent = "Copied!";
      setTimeout(() => e.target.textContent = "Copy Active Code", 1500);
    } catch (_) {}
  });
  document.getElementById("download-code").addEventListener("click", () => {
    downloadBlob(codeFilenames[activeCodeKey] || "artifact.txt", codeAssets[activeCodeKey] || "", "text/plain;charset=utf-8");
  });
  renderCodePreview();

  // RELATIONSHIPS TABLE
  document.getElementById("rel-table").innerHTML = `
    <thead><tr><th>Source</th><th>Target</th><th>Kind</th><th>Confidence</th><th>Overlap</th><th>Evidence</th></tr></thead>
    <tbody>
      ${data.relationships.length ? data.relationships.map(r => `
        <tr>
          <td class="mono"><span class="clickable" data-jump="${esc(r.source_table)}">${esc(r.source_table)}</span>.${esc(r.source_column)}</td>
          <td class="mono"><span class="clickable" data-jump="${esc(r.target_table)}">${esc(r.target_table)}</span>.${esc(r.target_column)}</td>
          <td><span class="badge ${r.kind === 'explicit_fk' ? 'b-green' : 'b-amber'}">${esc(r.kind)}</span></td>
          <td class="mono">${Math.round(r.confidence * 100)}%</td>
          <td class="mono">${r.overlap_rate != null ? Math.round(r.overlap_rate * 100) + '%' : '—'}</td>
          <td>${esc(r.evidence.join('; '))}</td>
        </tr>`).join("") : `<tr><td colspan="6" style="color:var(--text-muted)">No relationships discovered.</td></tr>`}
    </tbody>`;

  // INTERACTIVE ERD (with pan, wheel-zoom, search highlight, fit view & SVG export)
  let erdInitialized = false, erdNodes = {}, erdView = { x: 20, y: 20, scale: 1 };
  function initERDNodes() {
    erdNodes = {};
    const colsPerRow = Math.max(2, Math.ceil(Math.sqrt(baseTables.length)));
    const colW = 270, gapX = 70, gapY = 50;
    const colHeights = new Array(colsPerRow).fill(30);
    baseTables.forEach((t, i) => {
      const colIdx = i % colsPerRow;
      const shownCols = Math.min(t.columns.length, 10);
      const h = 44 + shownCols * 21 + (t.columns.length > 10 ? 20 : 0);
      erdNodes[t.full_name] = { table: t, x: 30 + colIdx * (colW + gapX), y: colHeights[colIdx], w: colW, h };
      colHeights[colIdx] += h + gapY;
    });
    erdInitialized = true;
  }

  function fitERD() {
    if (!erdInitialized) initERDNodes();
    const wrap = document.getElementById("erd-wrap");
    const vals = Object.values(erdNodes);
    if (!vals.length || !wrap.clientWidth) return;
    const minX = Math.min(...vals.map(n => n.x));
    const minY = Math.min(...vals.map(n => n.y));
    const maxX = Math.max(...vals.map(n => n.x + n.w));
    const maxY = Math.max(...vals.map(n => n.y + n.h));
    const pad = 36;
    const scale = Math.min(1.2, Math.max(0.25, Math.min((wrap.clientWidth - pad * 2) / Math.max(1, maxX - minX), (wrap.clientHeight - pad * 2) / Math.max(1, maxY - minY))));
    erdView = { x: pad - minX * scale, y: pad - minY * scale, scale };
    renderERD();
  }

  function renderERD() {
    if (!erdInitialized) initERDNodes();
    const svg = document.getElementById("erd-svg");
    const showInferred = document.getElementById("erd-show-inferred").checked;
    const q = (document.getElementById("erd-search").value || "").toLowerCase().trim();
    const rels = data.relationships.filter(r => showInferred || r.kind === "explicit_fk");

    const matchedNodes = new Set();
    if (q) {
      Object.values(erdNodes).forEach(n => {
        const t = n.table;
        if (t.full_name.toLowerCase().includes(q) || (t.semantic_name || "").toLowerCase().includes(q)) {
          matchedNodes.add(t.full_name);
        }
      });
    }

    let edgeSvg = "";
    rels.forEach(r => {
      const a = erdNodes[r.source_table], b = erdNodes[r.target_table];
      if (!a || !b) return;
      const startX = (a.x + a.w/2) < (b.x + b.w/2) ? a.x + a.w : a.x;
      const endX = (a.x + a.w/2) < (b.x + b.w/2) ? b.x : b.x + b.w;
      const startY = a.y + 34, endY = b.y + 34, cx = (startX + endX) / 2;
      const isExp = r.kind === "explicit_fk";
      const stroke = isExp ? "#34d399" : "#fbbf24";
      const dash = isExp ? "" : 'stroke-dasharray="6,4"';
      const label = `${r.source_column} → ${r.target_column}${isExp ? '' : ' (' + Math.round(r.confidence*100) + '%)'}`;
      const highlighted = !q || matchedNodes.has(r.source_table) || matchedNodes.has(r.target_table);
      const opacity = highlighted ? "0.9" : "0.18";
      edgeSvg += `<path d="M ${startX} ${startY} C ${cx} ${startY}, ${cx} ${endY}, ${endX} ${endY}" fill="none" stroke="${stroke}" stroke-width="2" ${dash} opacity="${opacity}" />
                  <circle cx="${endX}" cy="${endY}" r="4" fill="${stroke}" opacity="${opacity}" />
                  <text x="${cx}" y="${(startY + endY)/2 - 5}" text-anchor="middle" fill="${stroke}" font-size="10.5" font-family="monospace" opacity="${opacity}">${esc(label)}</text>`;
    });

    let nodeSvg = "";
    Object.values(erdNodes).forEach(n => {
      const t = n.table, shown = t.columns.slice(0, 10);
      const isMatch = q && matchedNodes.has(t.full_name);
      const dimmed = q && !isMatch;
      const borderStroke = isMatch ? "var(--accent)" : "var(--border)";
      const borderWidth = isMatch ? "2.6" : "1.5";
      const rows = shown.map((c, idx) => {
        const badge = c.is_primary_key ? "PK" : c.is_foreign_key ? "FK" : "";
        const bColor = c.is_primary_key ? "#34d399" : "#38bdf8";
        return `<text x="${n.x + 12}" y="${n.y + 52 + idx * 21}" fill="var(--text)" font-size="11.5" font-family="monospace">${esc(c.name.slice(0,22))}</text>
                <text x="${n.x + n.w - 10}" y="${n.y + 52 + idx * 21}" text-anchor="end" fill="${badge ? bColor : 'var(--text-muted)'}" font-size="10.5" font-family="monospace">${badge || esc(c.canonical_type)}</text>`;
      }).join("");
      const more = t.columns.length > 10 ? `<text x="${n.x + 12}" y="${n.y + n.h - 8}" fill="var(--text-muted)" font-size="10.5">+ ${t.columns.length - 10} more...</text>` : "";
      nodeSvg += `<g class="erd-node" data-id="${esc(t.full_name)}" style="cursor:move;" opacity="${dimmed ? '0.28' : '1'}">
        <rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="8" fill="var(--bg-card)" stroke="${borderStroke}" stroke-width="${borderWidth}" />
        <rect x="${n.x}" y="${n.y}" width="${n.w}" height="32" rx="8" fill="var(--bg-elev)" />
        <text x="${n.x + 12}" y="${n.y + 21}" fill="var(--accent)" font-size="12.5" font-weight="700" font-family="monospace" class="erd-title" style="cursor:pointer;">${esc(t.name)}</text>
        <text x="${n.x + n.w - 10}" y="${n.y + 20}" text-anchor="end" fill="var(--text-muted)" font-size="10.5" font-family="monospace">${fmtInt(t.row_count)}</text>
        ${rows}${more}</g>`;
    });

    svg.innerHTML = `<g id="erd-vp" transform="translate(${erdView.x},${erdView.y}) scale(${erdView.scale})">${edgeSvg}${nodeSvg}</g>`;
    bindERDDrag();
  }

  let dragState = null;
  function bindERDDrag() {
    document.getElementById("erd-svg").querySelectorAll(".erd-node").forEach(el => {
      el.addEventListener("mousedown", (e) => {
        e.stopPropagation();
        if (e.target.classList.contains("erd-title")) { jumpToTable(el.dataset.id); return; }
        const n = erdNodes[el.dataset.id];
        dragState = { type: "node", node: n, sx: e.clientX, sy: e.clientY, ox: n.x, oy: n.y };
      });
    });
  }
  document.getElementById("erd-wrap").addEventListener("mousedown", (e) => {
    dragState = { type: "erd-pan", sx: e.clientX, sy: e.clientY, ox: erdView.x, oy: erdView.y };
  });
  document.getElementById("erd-wrap").addEventListener("wheel", (e) => {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.1 : 0.9;
    erdView.scale = Math.min(2.5, Math.max(0.25, erdView.scale * factor));
    const vp = document.getElementById("erd-vp");
    if (vp) vp.setAttribute("transform", `translate(${erdView.x},${erdView.y}) scale(${erdView.scale})`);
  }, { passive: false });

  // LINEAGE DAG (with pan, wheel-zoom, search highlight, fit view, cycle guard & SVG export)
  let linView = { x: 16, y: 16, scale: 1 };
  let linBounds = { minX: 0, minY: 0, maxX: 400, maxY: 200 };

  function fitLineage() {
    const wrap = document.getElementById("lin-wrap");
    if (!wrap.clientWidth) return;
    const pad = 32;
    const w = Math.max(1, linBounds.maxX - linBounds.minX);
    const h = Math.max(1, linBounds.maxY - linBounds.minY);
    const scale = Math.min(1.2, Math.max(0.25, Math.min((wrap.clientWidth - pad * 2) / w, (wrap.clientHeight - pad * 2) / h)));
    linView = { x: pad - linBounds.minX * scale, y: pad - linBounds.minY * scale, scale };
    const vp = document.getElementById("lin-vp");
    if (vp) vp.setAttribute("transform", `translate(${linView.x},${linView.y}) scale(${linView.scale})`);
  }

  function renderLineage() {
    const svg = document.getElementById("lin-svg");
    const includeFk = document.getElementById("lin-include-fk").checked;
    const q = (document.getElementById("lin-search").value || "").toLowerCase().trim();
    const edges = data.lineage.filter(e => includeFk || e.edge_type === "view_dependency");
    if (!edges.length) {
      svg.innerHTML = `<text x="30" y="45" fill="var(--text-muted)" font-size="14">No view lineage edges found. Toggle "Include Foreign Key Dependencies" above to view table FK lineage.</text>`;
      return;
    }

    const typeMap = {};
    data.tables.forEach(t => { typeMap[t.full_name] = t.table_type; });
    const nodesSet = new Set();
    edges.forEach(e => { nodesSet.add(e.upstream_table); nodesSet.add(e.downstream_table); });
    const depth = {}, nodes = [...nodesSet];
    nodes.forEach(n => { depth[n] = 0; });
    // Break cycles and skip self-loops before computing DAG layer depths
    const adj = {};
    nodes.forEach(n => { adj[n] = []; });
    edges.forEach((e, idx) => {
      if (e.upstream_table !== e.downstream_table) {
        adj[e.upstream_table].push({ to: e.downstream_table, idx });
      }
    });
    const colorState = {}; // 0=unvisited, 1=visiting, 2=done
    const backEdges = new Set();
    function dfs(u) {
      colorState[u] = 1;
      for (const { to, idx } of (adj[u] || [])) {
        if (colorState[to] === 1) {
          backEdges.add(idx);
        } else if (!colorState[to]) {
          dfs(to);
        }
      }
      colorState[u] = 2;
    }
    nodes.forEach(n => { if (!colorState[n]) dfs(n); });
    const dagEdges = edges.filter((e, idx) => e.upstream_table !== e.downstream_table && !backEdges.has(idx));
    for (let iter = 0; iter < nodes.length; iter++) {
      let changed = false;
      dagEdges.forEach(e => {
        if ((depth[e.downstream_table] || 0) <= (depth[e.upstream_table] || 0)) {
          depth[e.downstream_table] = (depth[e.upstream_table] || 0) + 1;
          changed = true;
        }
      });
      if (!changed) break;
    }
    const layers = {};
    nodes.forEach(n => { (layers[depth[n]] = layers[depth[n]] || []).push(n); });
    const pos = {}, nodeW = 220, nodeH = 44;
    Object.keys(layers).sort((a,b) => Number(a) - Number(b)).forEach(d => {
      layers[d].sort().forEach((n, idx) => { pos[n] = { x: 35 + Number(d) * 300, y: 35 + idx * 72 }; });
    });

    const pVals = Object.values(pos);
    linBounds = {
      minX: Math.min(...pVals.map(p => p.x)),
      minY: Math.min(...pVals.map(p => p.y)) - 24,
      maxX: Math.max(...pVals.map(p => p.x + nodeW)),
      maxY: Math.max(...pVals.map(p => p.y + nodeH)),
    };

    const matched = new Set(q ? nodes.filter(n => n.toLowerCase().includes(q)) : []);

    let edgeSvg = "";
    edges.forEach((e, idx) => {
      const a = pos[e.upstream_table], b = pos[e.downstream_table];
      if (!a || !b) return;
      const color = e.edge_type === "view_dependency" ? "#a78bfa" : "#38bdf8";
      const opacity = (!q || matched.has(e.upstream_table) || matched.has(e.downstream_table)) ? "0.88" : "0.18";
      if (e.upstream_table === e.downstream_table) {
        const lx = a.x + nodeW - 24, ly = a.y;
        edgeSvg += `<path d="M ${lx - 26} ${ly} C ${lx - 26} ${ly - 26}, ${lx + 18} ${ly - 26}, ${lx + 10} ${ly}" fill="none" stroke="${color}" stroke-width="2" stroke-dasharray="4 3" opacity="${opacity}" /><circle cx="${lx + 10}" cy="${ly}" r="3.5" fill="${color}" opacity="${opacity}" />`;
        return;
      }
      const sx = a.x + nodeW, sy = a.y + nodeH / 2, ex = b.x, ey = b.y + nodeH / 2, cx = (sx + ex) / 2;
      const dash = backEdges.has(idx) ? ' stroke-dasharray="4 3"' : "";
      edgeSvg += `<path d="M ${sx} ${sy} C ${cx} ${sy}, ${cx} ${ey}, ${ex} ${ey}" fill="none" stroke="${color}" stroke-width="2"${dash} opacity="${opacity}" /><circle cx="${ex}" cy="${ey}" r="4" fill="${color}" opacity="${opacity}" />`;
    });

    let nodeSvg = "";
    nodes.forEach(n => {
      const p = pos[n], isView = typeMap[n] && typeMap[n] !== "table";
      const stroke = isView ? "#a78bfa" : "#38bdf8";
      const label = n.includes(".") ? n.split(".").slice(1).join(".") : n;
      const isMatch = q && matched.has(n);
      const dimmed = q && !isMatch;
      nodeSvg += `<g style="cursor:pointer;" data-jump="${esc(n)}" opacity="${dimmed ? '0.28' : '1'}">
        <rect x="${p.x}" y="${p.y}" width="${nodeW}" height="${nodeH}" rx="8" fill="var(--bg-card)" stroke="${stroke}" stroke-width="${isMatch ? 3 : 1.8}" />
        <text x="${p.x + 12}" y="${p.y + 20}" fill="var(--text)" font-size="12.5" font-weight="700" font-family="monospace">${esc(label)}</text>
        <text x="${p.x + 12}" y="${p.y + 35}" fill="var(--text-muted)" font-size="10.5" font-family="monospace">${esc(typeMap[n] || 'external')} · ${esc(n)}</text>
      </g>`;
    });

    svg.innerHTML = `<g id="lin-vp" transform="translate(${linView.x},${linView.y}) scale(${linView.scale})">${edgeSvg}${nodeSvg}</g>`;
  }

  document.getElementById("lin-wrap").addEventListener("mousedown", (e) => {
    if (e.target.closest("[data-jump]")) return;
    dragState = { type: "lin-pan", sx: e.clientX, sy: e.clientY, ox: linView.x, oy: linView.y };
  });
  document.getElementById("lin-wrap").addEventListener("wheel", (e) => {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.1 : 0.9;
    linView.scale = Math.min(2.5, Math.max(0.25, linView.scale * factor));
    const vp = document.getElementById("lin-vp");
    if (vp) vp.setAttribute("transform", `translate(${linView.x},${linView.y}) scale(${linView.scale})`);
  }, { passive: false });

  window.addEventListener("mousemove", (e) => {
    if (!dragState) return;
    if (dragState.type === "node") {
      dragState.node.x = dragState.ox + (e.clientX - dragState.sx) / erdView.scale;
      dragState.node.y = dragState.oy + (e.clientY - dragState.sy) / erdView.scale;
      renderERD();
    } else if (dragState.type === "erd-pan") {
      erdView.x = dragState.ox + (e.clientX - dragState.sx);
      erdView.y = dragState.oy + (e.clientY - dragState.sy);
      const vp = document.getElementById("erd-vp");
      if (vp) vp.setAttribute("transform", `translate(${erdView.x},${erdView.y}) scale(${erdView.scale})`);
    } else if (dragState.type === "lin-pan") {
      linView.x = dragState.ox + (e.clientX - dragState.sx);
      linView.y = dragState.oy + (e.clientY - dragState.sy);
      const vp = document.getElementById("lin-vp");
      if (vp) vp.setAttribute("transform", `translate(${linView.x},${linView.y}) scale(${linView.scale})`);
    }
  });
  window.addEventListener("mouseup", () => { dragState = null; });

  document.getElementById("erd-show-inferred").addEventListener("change", renderERD);
  document.getElementById("erd-search").addEventListener("input", renderERD);
  document.getElementById("erd-zoom-in").addEventListener("click", () => { erdView.scale = Math.min(2.5, erdView.scale * 1.15); renderERD(); });
  document.getElementById("erd-zoom-out").addEventListener("click", () => { erdView.scale = Math.max(0.25, erdView.scale / 1.15); renderERD(); });
  document.getElementById("erd-fit").addEventListener("click", fitERD);
  document.getElementById("erd-reset").addEventListener("click", () => { erdView = { x: 20, y: 20, scale: 1 }; erdInitialized = false; renderERD(); });
  document.getElementById("erd-download-svg").addEventListener("click", () => exportSvgElement(document.getElementById("erd-svg"), `${ov.database_name}_erd.svg`));

  document.getElementById("lin-include-fk").addEventListener("change", renderLineage);
  document.getElementById("lin-search").addEventListener("input", renderLineage);
  document.getElementById("lin-zoom-in").addEventListener("click", () => { linView.scale = Math.min(2.5, linView.scale * 1.15); renderLineage(); });
  document.getElementById("lin-zoom-out").addEventListener("click", () => { linView.scale = Math.max(0.25, linView.scale / 1.15); renderLineage(); });
  document.getElementById("lin-fit").addEventListener("click", fitLineage);
  document.getElementById("lin-reset").addEventListener("click", () => { linView = { x: 16, y: 16, scale: 1 }; renderLineage(); });
  document.getElementById("lin-download-svg").addEventListener("click", () => exportSvgElement(document.getElementById("lin-svg"), `${ov.database_name}_lineage.svg`));
})();
</script>
</body>
</html>
"""


def render_html_report(result: DiscoveryResult) -> str:
    """Render a standalone, offline-capable interactive HTML discovery report."""
    payload = {
        "result": result.model_dump(mode="json"),
        "dbt_yaml": render_dbt_sources_yaml(result),
        "uc_ddl": render_unity_catalog_ddl(result),
        "lakeflow_py": render_lakeflow_pipeline_py(result),
    }
    raw_json = json.dumps(payload).replace("</", "<\\/")
    return (
        HTML_TEMPLATE.replace("__TITLE__", html.escape(result.overview.database_name))
        .replace("__PAYLOAD_JSON__", raw_json)
    )


def export_html(result: DiscoveryResult, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_html_report(result), encoding="utf-8")
    return path
