"""Realistic demo database generator (SQLite or DuckDB) for testing tick discovery."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

from tick.dialects import resolve_connection_url


def create_demo_database(output_path: str | Path) -> Path:
    """Create a realistic e-commerce + telemetry database showcasing all tick features."""
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    url = resolve_connection_url(str(path))
    is_duckdb = url.startswith("duckdb:")
    engine = create_engine(url, future=True)

    rng = random.Random(42)
    now = datetime.now(timezone.utc)

    first_names = ["Alice", "Bob", "Carlos", "Diana", "Elena", "Frank", "Grace", "Hiro", "Iris", "Jamal"]
    last_names = ["Chen", "Patel", "Rivera", "Kim", "Okafor", "Lindqvist", "Rossi", "Smith", "Vargas", "Wu"]
    domains = ["acme.io", "northwind.dev", "example.com", "shopmail.org"]
    categories = ["Electronics", "Home", "Apparel", "Books", "Fitness"]
    statuses = ["completed", "shipped", "processing", "cancelled"]
    event_types = ["page_view", "add_to_cart", "checkout_start", "search", "purchase"]
    countries = ["US", "CA", "GB", "DE", "FR", "JP"]

    with engine.begin() as conn:
        # 1. customers (PK, PII: email, phone, ssn, full_name, watermarks, high-null deleted_at)
        conn.execute(
            text(
                """
                CREATE TABLE customers (
                    id INTEGER PRIMARY KEY,
                    full_name VARCHAR(120) NOT NULL,
                    email VARCHAR(160) NOT NULL UNIQUE,
                    phone VARCHAR(40),
                    ssn VARCHAR(20),
                    country VARCHAR(4) NOT NULL,
                    tier VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    deleted_at TIMESTAMP
                )
                """
            )
        )

        # 2. products (dimension table)
        conn.execute(
            text(
                """
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    sku VARCHAR(32) NOT NULL UNIQUE,
                    product_name VARCHAR(120) NOT NULL,
                    category VARCHAR(40) NOT NULL,
                    unit_price DECIMAL(10, 2) NOT NULL,
                    unit_cost DECIMAL(10, 2) NOT NULL,
                    is_active BOOLEAN NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
        )

        # 3. orders (fact table with explicit FK to customers, shipping_address PII)
        conn.execute(
            text(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER NOT NULL REFERENCES customers(id),
                    status VARCHAR(24) NOT NULL,
                    total_amount DECIMAL(12, 2) NOT NULL,
                    discount_amount DECIMAL(10, 2),
                    shipping_address VARCHAR(200),
                    currency VARCHAR(8) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
        )

        # 4. order_items (explicit FK to orders, UNENFORCED FK product_id -> tests relationship inference!)
        conn.execute(
            text(
                """
                CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER NOT NULL REFERENCES orders(id),
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    line_total DECIMAL(12, 2) NOT NULL
                )
                """
            )
        )

        # 5. click_events (NO primary key! candidate PK event_id, UNENFORCED customer_id, ip_address PII, 100% null column)
        conn.execute(
            text(
                """
                CREATE TABLE click_events (
                    event_id VARCHAR(40) NOT NULL,
                    customer_id INTEGER,
                    event_type VARCHAR(40) NOT NULL,
                    ip_address VARCHAR(45),
                    page_url VARCHAR(255) NOT NULL,
                    legacy_experiment_tag VARCHAR(64),
                    occurred_at TIMESTAMP NOT NULL
                )
                """
            )
        )

        # 6. legacy_imports (STALE table - last loaded 140 days ago)
        conn.execute(
            text(
                """
                CREATE TABLE legacy_imports (
                    import_id INTEGER PRIMARY KEY,
                    batch_source VARCHAR(40) NOT NULL,
                    records_loaded INTEGER NOT NULL,
                    loaded_at TIMESTAMP NOT NULL
                )
                """
            )
        )

        # 7. empty_staging_queue (empty table)
        conn.execute(
            text(
                """
                CREATE TABLE empty_staging_queue (
                    payload_id VARCHAR(40),
                    raw_payload TEXT,
                    ingested_at TIMESTAMP
                )
                """
            )
        )

        # Index on orders.created_at (leaving orders.customer_id intentionally unindexed to surface DE hint!)
        if not is_duckdb:
            conn.execute(text("CREATE INDEX idx_orders_created_at ON orders(created_at)"))
            conn.execute(text("CREATE INDEX idx_order_items_order_id ON order_items(order_id)"))

        # Seed customers (60 rows)
        customer_rows = []
        for cid in range(1, 61):
            fn = rng.choice(first_names)
            ln = rng.choice(last_names)
            created = now - timedelta(days=rng.randint(15, 365))
            updated = created + timedelta(days=rng.randint(0, 14))
            deleted = (updated + timedelta(days=2)).isoformat(sep=" ", timespec="seconds") if cid % 12 == 0 else None
            customer_rows.append(
                {
                    "id": cid,
                    "full_name": f"{fn} {ln}",
                    "email": f"{fn.lower()}.{ln.lower()}{cid}@{rng.choice(domains)}",
                    "phone": f"+1-555-{rng.randint(200,999)}-{rng.randint(1000,9999)}",
                    "ssn": f"{rng.randint(100,899)}-{rng.randint(10,99)}-{rng.randint(1000,9999)}",
                    "country": rng.choice(countries),
                    "tier": rng.choice(["free", "pro", "enterprise"]),
                    "created_at": created.isoformat(sep=" ", timespec="seconds"),
                    "updated_at": updated.isoformat(sep=" ", timespec="seconds"),
                    "deleted_at": deleted,
                }
            )
        conn.execute(
            text(
                """
                INSERT INTO customers (id, full_name, email, phone, ssn, country, tier, created_at, updated_at, deleted_at)
                VALUES (:id, :full_name, :email, :phone, :ssn, :country, :tier, :created_at, :updated_at, :deleted_at)
                """
            ),
            customer_rows,
        )

        # Seed products (25 rows)
        product_rows = []
        for pid in range(1, 26):
            price = round(rng.uniform(12.0, 299.0), 2)
            product_rows.append(
                {
                    "id": pid,
                    "sku": f"SKU-{pid:04d}",
                    "product_name": f"Product Item {pid}",
                    "category": rng.choice(categories),
                    "unit_price": price,
                    "unit_cost": round(price * rng.uniform(0.35, 0.65), 2),
                    "is_active": pid % 9 != 0,
                    "updated_at": (now - timedelta(days=rng.randint(1, 25))).isoformat(sep=" ", timespec="seconds"),
                }
            )
        conn.execute(
            text(
                """
                INSERT INTO products (id, sku, product_name, category, unit_price, unit_cost, is_active, updated_at)
                VALUES (:id, :sku, :product_name, :category, :unit_price, :unit_cost, :is_active, :updated_at)
                """
            ),
            product_rows,
        )

        # Seed orders (220 rows) + order_items (480 rows)
        order_rows = []
        item_rows = []
        item_id = 1
        for oid in range(1, 221):
            cid = rng.randint(1, 60)
            created = now - timedelta(days=rng.randint(0, 60), hours=rng.randint(0, 23))
            updated = created + timedelta(hours=rng.randint(1, 36))
            n_items = rng.randint(1, 4)
            order_total = 0.0
            for _ in range(n_items):
                pid = rng.randint(1, 25)
                qty = rng.randint(1, 3)
                line_total = round(product_rows[pid - 1]["unit_price"] * qty, 2)
                order_total += line_total
                item_rows.append(
                    {
                        "id": item_id,
                        "order_id": oid,
                        "product_id": pid,
                        "quantity": qty,
                        "line_total": line_total,
                    }
                )
                item_id += 1

            order_rows.append(
                {
                    "id": oid,
                    "customer_id": cid,
                    "status": rng.choice(statuses),
                    "total_amount": round(order_total, 2),
                    "discount_amount": round(order_total * 0.1, 2) if oid % 5 == 0 else None,
                    "shipping_address": f"{rng.randint(10, 999)} Market St, Apt {rng.randint(1, 40)}",
                    "currency": "USD",  # constant column!
                    "created_at": created.isoformat(sep=" ", timespec="seconds"),
                    "updated_at": updated.isoformat(sep=" ", timespec="seconds"),
                }
            )
        conn.execute(
            text(
                """
                INSERT INTO orders (id, customer_id, status, total_amount, discount_amount, shipping_address, currency, created_at, updated_at)
                VALUES (:id, :customer_id, :status, :total_amount, :discount_amount, :shipping_address, :currency, :created_at, :updated_at)
                """
            ),
            order_rows,
        )
        conn.execute(
            text(
                """
                INSERT INTO order_items (id, order_id, product_id, quantity, line_total)
                VALUES (:id, :order_id, :product_id, :quantity, :line_total)
                """
            ),
            item_rows,
        )

        # Seed click_events (350 rows)
        event_rows = []
        for eid in range(1, 351):
            event_rows.append(
                {
                    "event_id": f"evt_{eid:06d}",
                    "customer_id": rng.randint(1, 60) if eid % 4 != 0 else None,
                    "event_type": rng.choice(event_types),
                    "ip_address": f"192.168.{rng.randint(1, 254)}.{rng.randint(1, 254)}",
                    "page_url": f"https://shop.acme.io/{rng.choice(['home', 'catalog', 'cart', 'checkout'])}",
                    "legacy_experiment_tag": None,
                    "occurred_at": (now - timedelta(hours=rng.randint(0, 240))).isoformat(sep=" ", timespec="seconds"),
                }
            )
        conn.execute(
            text(
                """
                INSERT INTO click_events (event_id, customer_id, event_type, ip_address, page_url, legacy_experiment_tag, occurred_at)
                VALUES (:event_id, :customer_id, :event_type, :ip_address, :page_url, :legacy_experiment_tag, :occurred_at)
                """
            ),
            event_rows,
        )

        # Seed legacy_imports (15 stale rows)
        legacy_rows = [
            {
                "import_id": i,
                "batch_source": "sftp_csv",
                "records_loaded": rng.randint(100, 5000),
                "loaded_at": (now - timedelta(days=140 + i)).isoformat(sep=" ", timespec="seconds"),
            }
            for i in range(1, 16)
        ]
        conn.execute(
            text(
                """
                INSERT INTO legacy_imports (import_id, batch_source, records_loaded, loaded_at)
                VALUES (:import_id, :batch_source, :records_loaded, :loaded_at)
                """
            ),
            legacy_rows,
        )

        # Views (multi-hop lineage!)
        conn.execute(
            text(
                """
                CREATE VIEW v_order_summary AS
                SELECT
                    o.id AS order_id,
                    o.customer_id,
                    c.tier AS customer_tier,
                    o.status,
                    o.total_amount,
                    COUNT(oi.id) AS item_count,
                    o.created_at
                FROM orders o
                JOIN customers c ON c.id = o.customer_id
                LEFT JOIN order_items oi ON oi.order_id = o.id
                GROUP BY o.id, o.customer_id, c.tier, o.status, o.total_amount, o.created_at
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE VIEW v_customer_ltv AS
                SELECT
                    c.id AS customer_id,
                    c.country,
                    c.tier,
                    COUNT(os.order_id) AS order_count,
                    COALESCE(SUM(os.total_amount), 0) AS lifetime_value
                FROM customers c
                LEFT JOIN v_order_summary os ON os.customer_id = c.id
                GROUP BY c.id, c.country, c.tier
                """
            )
        )

    engine.dispose()
    return path


def create_manufacturing_demo_database(
    output_path: str | Path,
    plant_variant: str = "plant_ohio",
) -> Path:
    """Create a Smart Factory / SAP + MES + OT Historian + BOM demo database (SQLite or DuckDB)."""
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    url = resolve_connection_url(str(path))
    engine = create_engine(url, future=True)

    rng = random.Random(101 if plant_variant == "plant_ohio" else 202)
    now = datetime.now(timezone.utc)
    is_munich = plant_variant != "plant_ohio"
    plant_code = "DE02" if is_munich else "US01"

    with engine.begin() as conn:
        # 1. SAP MARA (Material Master — cryptic names + commercial sensitive STPRS cost)
        conn.execute(
            text(
                """
                CREATE TABLE MARA (
                    MANDT VARCHAR(3) NOT NULL,
                    MATNR VARCHAR(18) PRIMARY KEY,
                    MTART VARCHAR(4) NOT NULL,
                    MEINS VARCHAR(4) NOT NULL,
                    STPRS DECIMAL(12, 2) NOT NULL,
                    ERDAT INTEGER NOT NULL
                )
                """
            )
        )

        # 2. SAP AUFK (Production Orders — plant isolation WERKS + split ERDAT/ERZET integer date+time!)
        conn.execute(
            text(
                """
                CREATE TABLE AUFK (
                    AUFNR VARCHAR(16) PRIMARY KEY,
                    WERKS VARCHAR(6) NOT NULL,
                    MATNR VARCHAR(18) NOT NULL,
                    AUART VARCHAR(6) NOT NULL,
                    ERDAT INTEGER NOT NULL,
                    ERZET INTEGER NOT NULL,
                    AEDAT INTEGER NOT NULL
                )
                """
            )
        )

        # 3. MES Work Orders (Spaces in column names -> Delta linter!, ITAR/CAD path, unit_cost, site drift!)
        if is_munich:
            conn.execute(
                text(
                    """
                    CREATE TABLE work_orders (
                        "Order #" VARCHAR(24) PRIMARY KEY,
                        plant_code VARCHAR(8) NOT NULL,
                        material_number VARCHAR(18) NOT NULL,
                        "Qty/Hr" DECIMAL(10, 2) NOT NULL,
                        unit_cost DECIMAL(12, 2) NOT NULL,
                        cad_drawing_path VARCHAR(200),
                        status VARCHAR(20) NOT NULL,
                        eu_shift_supervisor VARCHAR(80),
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL
                    )
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE work_orders (
                        "Order #" INTEGER PRIMARY KEY,
                        plant_code VARCHAR(8) NOT NULL,
                        material_number VARCHAR(18) NOT NULL,
                        "Qty/Hr" DECIMAL(10, 2) NOT NULL,
                        unit_cost DECIMAL(12, 2) NOT NULL,
                        cad_drawing_path VARCHAR(200),
                        status VARCHAR(20) NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP NOT NULL
                    )
                    """
                )
            )

        # 4. Recursive Bill of Materials (multi-level parent_part_id -> child_part_id)
        conn.execute(
            text(
                """
                CREATE TABLE bill_of_materials (
                    bom_line_id INTEGER PRIMARY KEY,
                    parent_part_id VARCHAR(18) NOT NULL,
                    child_part_id VARCHAR(18) NOT NULL,
                    qty_per DECIMAL(10, 4) NOT NULL,
                    plant_id VARCHAR(8) NOT NULL
                )
                """
            )
        )

        # 5. Narrow OT Historian Tag Table (10-second polling + OPC quality codes with bad readings)
        conn.execute(
            text(
                """
                CREATE TABLE historian_tags (
                    tag_id VARCHAR(48) NOT NULL,
                    recorded_at TIMESTAMP NOT NULL,
                    value DOUBLE NOT NULL,
                    quality_code INTEGER NOT NULL
                )
                """
            )
        )

        # 6. Wide PLC Polling Table (with intentionally flatlined vibration_mm_s sensor!)
        conn.execute(
            text(
                """
                CREATE TABLE plc_press_telemetry (
                    press_id VARCHAR(16) NOT NULL,
                    recorded_at TIMESTAMP NOT NULL,
                    temp_c DOUBLE NOT NULL,
                    pressure_bar DOUBLE NOT NULL,
                    spindle_rpm INTEGER NOT NULL,
                    vibration_mm_s DOUBLE NOT NULL
                )
                """
            )
        )

        # 7. Empty SAP Z-table (tests --skip-empty-tables guardrail!)
        if not is_munich:
            conn.execute(
                text(
                    """
                    CREATE TABLE empty_sap_ztable (
                        MANDT VARCHAR(3),
                        ZKEY VARCHAR(20),
                        ZVAL VARCHAR(100)
                    )
                    """
                )
            )

        # Seed MARA (20 materials)
        mat_rows = []
        for i in range(1, 21):
            mtart = "FERT" if i <= 4 else ("HALB" if i <= 10 else "ROH")
            mat_rows.append(
                {
                    "MANDT": "100",
                    "MATNR": f"MAT-{i:05d}",
                    "MTART": mtart,
                    "MEINS": "EA" if i <= 10 else "KG",
                    "STPRS": round(rng.uniform(4.5, 450.0), 2),
                    "ERDAT": int((now - timedelta(days=rng.randint(10, 180))).strftime("%Y%m%d")),
                }
            )
        conn.execute(
            text("INSERT INTO MARA VALUES (:MANDT, :MATNR, :MTART, :MEINS, :STPRS, :ERDAT)"),
            mat_rows,
        )

        # Seed AUFK (45 production orders)
        aufk_rows = []
        for i in range(1, 46):
            dt = now - timedelta(days=rng.randint(0, 30), hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            upd = dt + timedelta(days=rng.randint(0, 3))
            aufk_rows.append(
                {
                    "AUFNR": f"000100{i:04d}",
                    "WERKS": plant_code,
                    "MATNR": f"MAT-{rng.randint(1, 10):05d}",
                    "AUART": rng.choice(["PP01", "PP02", "PM01"]),
                    "ERDAT": int(dt.strftime("%Y%m%d")),
                    "ERZET": int(dt.strftime("%H%M%S")),
                    "AEDAT": int(upd.strftime("%Y%m%d")),
                }
            )
        conn.execute(
            text("INSERT INTO AUFK VALUES (:AUFNR, :WERKS, :MATNR, :AUART, :ERDAT, :ERZET, :AEDAT)"),
            aufk_rows,
        )

        # Seed work_orders (50 rows)
        wo_rows = []
        for i in range(1, 51):
            created = now - timedelta(days=rng.randint(0, 20), hours=rng.randint(0, 23))
            updated = created + timedelta(hours=rng.randint(1, 12))
            row = {
                "order_num": f"DE-{i:05d}" if is_munich else i,
                "plant_code": plant_code,
                "material_number": f"MAT-{rng.randint(1, 10):05d}",
                "qty_hr": round(rng.uniform(20.0, 180.0), 2),
                "unit_cost": round(rng.uniform(15.0, 320.0), 2),
                "cad_drawing_path": f"s3://plm-vault/itar/DWG-{rng.randint(100,999)}.step",
                "status": rng.choice(["OPEN", "IN_PROGRESS", "COMPLETE", "HOLD"]),
                "created_at": created.isoformat(sep=" ", timespec="seconds"),
                "updated_at": updated.isoformat(sep=" ", timespec="seconds"),
            }
            if is_munich:
                row["eu_shift_supervisor"] = rng.choice(["K. Weber", "L. Bauer", "M. Hoffman"])
            wo_rows.append(row)

        if is_munich:
            conn.execute(
                text(
                    """
                    INSERT INTO work_orders ("Order #", plant_code, material_number, "Qty/Hr", unit_cost, cad_drawing_path, status, eu_shift_supervisor, created_at, updated_at)
                    VALUES (:order_num, :plant_code, :material_number, :qty_hr, :unit_cost, :cad_drawing_path, :status, :eu_shift_supervisor, :created_at, :updated_at)
                    """
                ),
                wo_rows,
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO work_orders ("Order #", plant_code, material_number, "Qty/Hr", unit_cost, cad_drawing_path, status, created_at, updated_at)
                    VALUES (:order_num, :plant_code, :material_number, :qty_hr, :unit_cost, :cad_drawing_path, :status, :created_at, :updated_at)
                    """
                ),
                wo_rows,
            )

        # Seed recursive multi-level BOM: FERT (1..4) -> HALB (5..10) -> ROH (11..20)
        bom_rows = []
        bid = 1
        for fert in range(1, 5):
            for halb in range(5, 8):
                bom_rows.append(
                    {
                        "bom_line_id": bid,
                        "parent_part_id": f"MAT-{fert:05d}",
                        "child_part_id": f"MAT-{halb:05d}",
                        "qty_per": float(rng.randint(1, 4)),
                        "plant_id": plant_code,
                    }
                )
                bid += 1
        for halb in range(5, 11):
            for roh in range(11, 14):
                bom_rows.append(
                    {
                        "bom_line_id": bid,
                        "parent_part_id": f"MAT-{halb:05d}",
                        "child_part_id": f"MAT-{roh:05d}",
                        "qty_per": round(rng.uniform(0.5, 5.0), 2),
                        "plant_id": plant_code,
                    }
                )
                bid += 1
        conn.execute(
            text("INSERT INTO bill_of_materials VALUES (:bom_line_id, :parent_part_id, :child_part_id, :qty_per, :plant_id)"),
            bom_rows,
        )

        # Seed historian_tags (180 rows, 10s intervals, 10% bad OPC quality codes < 192)
        hist_rows = []
        base_ts = now - timedelta(minutes=30)
        tags = ["LINE1.PRESS.TEMP", "LINE1.PRESS.BAR", "LINE2.OVEN.TEMP"]
        for i in range(180):
            ts = base_ts + timedelta(seconds=i * 10)
            hist_rows.append(
                {
                    "tag_id": tags[i % len(tags)],
                    "recorded_at": ts.isoformat(sep=" ", timespec="seconds"),
                    "value": round(rng.uniform(45.0, 210.0), 3),
                    "quality_code": 64 if i % 10 == 0 else 192,
                }
            )
        conn.execute(
            text("INSERT INTO historian_tags VALUES (:tag_id, :recorded_at, :value, :quality_code)"),
            hist_rows,
        )

        # Seed plc_press_telemetry (100 rows, vibration_mm_s flatlined at 0.42!)
        plc_rows = []
        for i in range(100):
            ts = base_ts + timedelta(seconds=i * 15)
            plc_rows.append(
                {
                    "press_id": f"PRESS-{(i % 3) + 1}",
                    "recorded_at": ts.isoformat(sep=" ", timespec="seconds"),
                    "temp_c": round(rng.uniform(62.0, 88.5), 2),
                    "pressure_bar": round(rng.uniform(110.0, 145.0), 2),
                    "spindle_rpm": rng.randint(1200, 1850),
                    "vibration_mm_s": 0.42,  # Stuck sensor!
                }
            )
        conn.execute(
            text("INSERT INTO plc_press_telemetry VALUES (:press_id, :recorded_at, :temp_c, :pressure_bar, :spindle_rpm, :vibration_mm_s)"),
            plc_rows,
        )

        # View reading MES + SAP tables
        conn.execute(
            text(
                """
                CREATE VIEW v_plant_oee_summary AS
                SELECT
                    w.plant_code,
                    w.material_number,
                    m.MTART AS material_type,
                    COUNT(*) AS order_count,
                    AVG(w."Qty/Hr") AS avg_qty_per_hr
                FROM work_orders w
                LEFT JOIN MARA m ON m.MATNR = w.material_number
                GROUP BY w.plant_code, w.material_number, m.MTART
                """
            )
        )

    engine.dispose()
    return path


def create_files_demo_landing_zone(output_dir: str | Path) -> Path:
    """Create a realistic multi-format Data Lake / SFTP landing zone (CSV, JSONL, Parquet, Hive partitions)."""
    import csv
    import json

    from tick.dialects import _module_installed

    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(303)
    now = datetime.now(timezone.utc)

    # 1. Flat CSV: work_orders.csv (with PII/governance & spaces in column names!)
    wo_csv = root / "work_orders.csv"
    with wo_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["Order #", "plant_code", "material_number", "unit_cost", "operator_email", "status", "updated_at"])
        for i in range(1, 31):
            w.writerow([
                1000 + i,
                "US01" if i % 2 == 0 else "DE02",
                f"MAT-{((i - 1) % 10) + 1:05d}",
                round(rng.uniform(12.5, 420.0), 2),
                f"operator{i}@acme-factory.com",
                rng.choice(["OPEN", "RELEASED", "COMPLETED"]),
                (now - timedelta(hours=i * 3)).isoformat(sep=" ", timespec="seconds"),
            ])

    # 2. NDJSON / JSONL: historian_tags.jsonl (OT telemetry with numeric OPC quality codes)
    hist_jsonl = root / "historian_tags.jsonl"
    base_ts = now - timedelta(hours=2)
    with hist_jsonl.open("w", encoding="utf-8") as fh:
        for i in range(60):
            rec = {
                "tag_id": f"LINE1.TEMP_{(i % 3) + 1}",
                "recorded_at": (base_ts + timedelta(seconds=i * 10)).isoformat(sep=" ", timespec="seconds"),
                "value": round(rng.uniform(70.0, 95.0), 3),
                "quality_code": 64 if i % 10 == 0 else 192,
            }
            fh.write(json.dumps(rec) + "\n")

    # 3. Subdirectory schema `sap/MARA.csv` (SAP legacy dates & commercial cost)
    sap_dir = root / "sap"
    sap_dir.mkdir(parents=True, exist_ok=True)
    with (sap_dir / "MARA.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["MANDT", "MATNR", "MTART", "MEINS", "STPRS", "ERDAT"])
        for i in range(1, 16):
            w.writerow(["100", f"MAT-{i:05d}", "FERT" if i <= 4 else "ROH", "EA", round(rng.uniform(5.0, 300.0), 2), 20260100 + i])

    # 4. Parquet BOM + Hive-partitioned Parquet telemetry (when DuckDB is installed)
    if _module_installed("duckdb"):
        import duckdb

        con = duckdb.connect(":memory:")
        try:
            bom_pq = root / "bill_of_materials.parquet"
            con.execute(
                """
                CREATE TABLE bom_tmp AS
                SELECT * FROM (VALUES
                    (1, 'MAT-00001', 'MAT-00005', 2.0, 'US01'),
                    (2, 'MAT-00001', 'MAT-00006', 4.0, 'US01'),
                    (3, 'MAT-00005', 'MAT-00011', 3.5, 'US01'),
                    (4, 'MAT-00002', 'MAT-00005', 1.0, 'DE02'),
                    (5, 'MAT-00005', 'MAT-00012', 2.5, 'DE02')
                ) AS t(bom_line_id, parent_part_id, child_part_id, qty_per, plant_id)
                """
            )
            con.execute(f"COPY bom_tmp TO '{str(bom_pq).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET)")

            for plant_val in ("US01", "DE02"):
                pdir = root / "telemetry" / f"plant={plant_val}"
                pdir.mkdir(parents=True, exist_ok=True)
                pfile = pdir / "part-0000.parquet"
                con.execute(
                    f"""
                    COPY (
                        SELECT
                            'PRESS-' || ((i % 2) + 1) AS press_id,
                            TIMESTAMP '2026-11-26 08:00:00' + INTERVAL (i * 15) SECOND AS recorded_at,
                            70.0 + (i % 15) AS temp_c,
                            120.0 + (i % 10) AS pressure_bar,
                            1500 + (i * 5) AS spindle_rpm
                        FROM range(1, 21) t(i)
                    ) TO '{str(pfile).replace(chr(39), chr(39)*2)}' (FORMAT PARQUET)
                    """
                )
        finally:
            con.close()

    return root
