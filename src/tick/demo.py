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
