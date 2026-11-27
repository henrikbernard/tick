"""ERP / SAP / MES semantic glossary loader and table/column enrichment."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tick.dialects import DialectAdapter, safe_rollback
from tick.models import TableInfo

# Built-in curated SAP ECC/S4 + Manufacturing ERP/MES glossary
BUILTIN_TABLE_GLOSSARY: dict[str, tuple[str, str]] = {
    "MARA": ("material_master", "SAP General Material Data (part/SKU master attributes)"),
    "MAKT": ("material_descriptions", "SAP Material Descriptions by language"),
    "MARC": ("plant_material_data", "SAP Plant Data for Material (MRP, lot size, lead times)"),
    "MARD": ("storage_location_stock", "SAP Storage Location Inventory Data for Material"),
    "STKO": ("bom_header", "SAP Bill of Materials (BOM) Header"),
    "STPO": ("bom_item", "SAP Bill of Materials (BOM) Component Items"),
    "AUFK": ("production_order_master", "SAP Production / Maintenance Order Master Data"),
    "AFKO": ("production_order_header", "SAP Production Order Header (PP orders)"),
    "AFPO": ("production_order_item", "SAP Production Order Item Quantities & Material"),
    "AFVC": ("production_order_operations", "SAP Order Routing Operations"),
    "AFRU": ("production_order_confirmations", "SAP Order Time & Yield Confirmations"),
    "CRHD": ("work_center_header", "SAP Work Center / Machine Header"),
    "RESB": ("material_reservations", "SAP Dependent Requirements / Component Reservations"),
    "MKPF": ("material_document_header", "SAP Inventory Movement Document Header"),
    "MSEG": ("material_document_segment", "SAP Inventory Movement Line Items (Goods Receipt/Issue)"),
    "QALS": ("quality_inspection_lot", "SAP Quality Management Inspection Lot Record"),
    "QAVE": ("quality_usage_decision", "SAP Quality Inspection Lot Usage Decision"),
    "EQUI": ("equipment_master", "SAP Plant Maintenance Equipment / Asset Master"),
    "IFLOT": ("functional_location", "SAP Plant Maintenance Functional Location Hierarchy"),
    "EKKO": ("purchase_order_header", "SAP Purchasing Document Header"),
    "EKPO": ("purchase_order_item", "SAP Purchasing Document Line Items"),
    "VBAK": ("sales_order_header", "SAP Sales Document Header"),
    "VBAP": ("sales_order_item", "SAP Sales Document Line Items"),
    "KNA1": ("customer_master", "SAP General Customer Master"),
    "LFA1": ("vendor_master", "SAP General Supplier / Vendor Master"),
    "BKPF": ("accounting_document_header", "SAP Financial Accounting Document Header"),
    "BSEG": ("accounting_document_segment", "SAP Financial Accounting Line Items"),
}

BUILTIN_COLUMN_GLOSSARY: dict[str, tuple[str, str]] = {
    "MANDT": ("client_id", "SAP Client Identifier"),
    "WERKS": ("plant_code", "Manufacturing Plant / Facility Code"),
    "BUKRS": ("company_code", "Legal Entity / Company Code"),
    "LGORT": ("storage_location", "Warehouse / Storage Location Code"),
    "MATNR": ("material_number", "Material / Part Number"),
    "MAKTX": ("material_description", "Material Short Description"),
    "MTART": ("material_type", "Material Type (RAW, HALB, FERT, etc.)"),
    "MEINS": ("base_unit_of_measure", "Base Unit of Measure (EA, KG, M, etc.)"),
    "MENGE": ("quantity", "Quantity in Base Unit of Measure"),
    "CHARG": ("batch_lot_number", "Production Batch / Lot Number"),
    "AUFNR": ("production_order_number", "Production / Work Order Number"),
    "AUART": ("order_type", "Production or Maintenance Order Type"),
    "ARBPL": ("work_center_code", "Shop-Floor Work Center / Line / Machine"),
    "VORNR": ("operation_number", "Routing Operation Step Number"),
    "ERDAT": ("created_date", "Record Creation Date (often YYYYMMDD)"),
    "ERZET": ("created_time", "Record Creation Time (often HHMMSS)"),
    "AEDAT": ("changed_date", "Last Changed Date (often YYYYMMDD)"),
    "AEZEIT": ("changed_time", "Last Changed Time (often HHMMSS)"),
    "BUDAT": ("posting_date", "Accounting / Inventory Posting Date"),
    "BLDAT": ("document_date", "Source Document Date"),
    "NETWR": ("net_value", "Net Document Value"),
    "WAERS": ("currency_code", "Currency Key (ISO)"),
    "KUNNR": ("customer_number", "Customer Account Number"),
    "LIFNR": ("vendor_number", "Supplier / Vendor Account Number"),
    "VBELN": ("sales_document_number", "Sales / Delivery Document Number"),
    "POSNR": ("line_item_number", "Document Line Item Number"),
    "EBELN": ("purchase_order_number", "Purchase Order Number"),
    "PRUEFLOS": ("inspection_lot_number", "Quality Inspection Lot Number"),
    "EQUNR": ("equipment_number", "Plant Asset / Equipment ID"),
    "TPLNR": ("functional_location_id", "Plant Functional Location Code"),
    "STLNR": ("bom_number", "Bill of Materials Identifier"),
    "IDNRK": ("bom_component_material", "BOM Component Material Number"),
    "STPRS": ("standard_cost_price", "Material Standard Cost"),
    "VERPR": ("moving_average_cost", "Material Moving Average Unit Cost"),
}


def load_custom_glossary(path: str | Path) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    """Load custom table/column semantic glossary from YAML or JSON.

    Expected format:
    tables:
      AUFK:
        name: work_orders
        description: Plant work orders
    columns:
      WERKS:
        name: plant_code
        description: Plant identifier
    """
    p = Path(path)
    raw_text = p.read_text(encoding="utf-8")
    data: dict[str, Any] = json.loads(raw_text) if p.suffix.lower() == ".json" else (yaml.safe_load(raw_text) or {})

    tbl_map: dict[str, tuple[str, str]] = {}
    col_map: dict[str, tuple[str, str]] = {}

    for k, v in (data.get("tables") or {}).items():
        if isinstance(v, str):
            tbl_map[k.upper()] = (v, v)
        elif isinstance(v, dict):
            tbl_map[k.upper()] = (str(v.get("name", k)), str(v.get("description", "")))

    for k, v in (data.get("columns") or {}).items():
        if isinstance(v, str):
            col_map[k.upper()] = (v, v)
        elif isinstance(v, dict):
            col_map[k.upper()] = (str(v.get("name", k)), str(v.get("description", "")))

    return tbl_map, col_map


def _try_extract_sap_ddic(
    engine: Engine,
    tables: list[TableInfo],
    adapter: DialectAdapter | None = None,
    default_schema: str | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """If SAP DD02T / DD03T data dictionary tables exist in the discovered DB, extract business descriptions."""
    table_names_u = {t.name.upper(): t for t in tables}
    tbl_desc: dict[str, str] = {}
    col_desc: dict[str, str] = {}

    if "DD02T" not in table_names_u and "DD03T" not in table_names_u:
        return tbl_desc, col_desc

    with engine.connect() as conn:
        if "DD02T" in table_names_u:
            dd02t_tbl = table_names_u["DD02T"]
            q_dd02t = (
                adapter.quote_table(conn, dd02t_tbl.schema_name, dd02t_tbl.name, default_schema)
                if adapter
                else dd02t_tbl.name
            )
            try:
                rows = conn.execute(
                    text(f"SELECT tabname, ddtext FROM {q_dd02t} WHERE ddlanguage = 'E'")
                ).fetchall()
                for tabname, ddtext in rows:
                    if tabname and ddtext:
                        tbl_desc[str(tabname).upper()] = str(ddtext).strip()
            except Exception:
                safe_rollback(conn)
        if "DD03T" in table_names_u:
            dd03t_tbl = table_names_u["DD03T"]
            q_dd03t = (
                adapter.quote_table(conn, dd03t_tbl.schema_name, dd03t_tbl.name, default_schema)
                if adapter
                else dd03t_tbl.name
            )
            try:
                rows = conn.execute(
                    text(f"SELECT fieldname, ddtext FROM {q_dd03t} WHERE ddlanguage = 'E'")
                ).fetchall()
                for fieldname, ddtext in rows:
                    if fieldname and ddtext:
                        col_desc[str(fieldname).upper()] = str(ddtext).strip()
            except Exception:
                safe_rollback(conn)

    return tbl_desc, col_desc


def enrich_with_glossary(
    engine: Engine,
    tables: list[TableInfo],
    glossary_path: str | None = None,
    adapter: DialectAdapter | None = None,
    default_schema: str | None = None,
) -> None:
    """Populate semantic_name and business_description using built-in ERP glossary, SAP DDIC, and user YAML."""
    tbl_glossary = dict(BUILTIN_TABLE_GLOSSARY)
    col_glossary = dict(BUILTIN_COLUMN_GLOSSARY)

    sap_t_desc, sap_c_desc = _try_extract_sap_ddic(
        engine, tables, adapter=adapter, default_schema=default_schema
    )

    if glossary_path:
        user_t, user_c = load_custom_glossary(glossary_path)
        tbl_glossary.update(user_t)
        col_glossary.update(user_c)

    for tbl in tables:
        key_u = tbl.name.upper()
        t_hit = tbl_glossary.get(key_u)
        if t_hit:
            tbl.semantic_name = t_hit[0]
            tbl.business_description = t_hit[1] or tbl.comment
        # SAP DD02T provides human descriptions without overwriting clean identifier semantic_name
        if key_u in sap_t_desc and not tbl.business_description:
            tbl.business_description = sap_t_desc[key_u]

        for col in tbl.columns:
            ckey_u = col.name.upper()
            c_hit = col_glossary.get(ckey_u)
            if c_hit:
                col.semantic_name = c_hit[0]
                col.business_description = c_hit[1] or col.comment
            if ckey_u in sap_c_desc and not col.business_description:
                col.business_description = sap_c_desc[ckey_u]
