from pathlib import Path
import pyarrow.parquet as pq

ROOT = Path.cwd()
SILVER = ROOT / "data" / "silver"

checks = {
    "products": [
        "unit_cost", "list_price", "vat_rate",
        "effective_from", "effective_to", "is_current"
    ],
    "orders": ["order_ts", "channel", "payment_method", "status"],
    "order_items": ["qty", "unit_price", "discount_rate", "net_amount"],
    "pos_sales": [
        "sale_ts", "qty", "unit_price", "discount_rate", "net_amount"
    ],
    "returns": ["return_ts", "return_qty", "reason", "return_amount"],
    "gl_entries": ["posting_date", "amount", "currency", "document_type"],
    "customers": ["signup_date", "region", "segment", "consent_marketing", "is_active"],
    "stores": ["city", "format", "is_active"],
}

for table, columns in checks.items():
    path = SILVER / f"{table}.parquet"
    print(f"\n{'=' * 18} {table} {'=' * 18}")
    if not path.exists():
        print(f"FICHIER INTROUVABLE : {path}")
        continue

    parquet = pq.ParquetFile(path)
    available = set(parquet.schema_arrow.names)

    for column in columns:
        if column not in available:
            print(f"{column}: COLONNE ABSENTE")
            continue

        values = parquet.read(columns=[column])[column]
        non_null = values.drop_null()
        print(f"\n{column}: nulls={len(values) - len(non_null)}")

        if column in {
            "unit_cost", "list_price", "vat_rate", "qty", "unit_price",
            "discount_rate", "net_amount", "return_qty", "return_amount", "amount"
        } and len(non_null):
            print(f"  min={non_null.to_pandas().min()} ; max={non_null.to_pandas().max()}")


        if column in {
            "channel", "payment_method", "status", "reason", "currency",
            "document_type", "region", "segment", "consent_marketing",
            "is_active", "is_current", "format"
        }:
            unique = non_null.unique().to_pylist()
            print(f"  valeurs distinctes ({len(unique)}): {unique[:40]}")
