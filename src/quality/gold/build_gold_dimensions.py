
from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold" / "dimensions"
REPORTS = ROOT / "data" / "reports" / "gold"

GOLD.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)


def read_silver(table):
    path = SILVER / f"{table}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Table Silver absente : {path}")
    return pd.read_parquet(path)


def save_dimension(df, name):
    path = GOLD / f"{name}.parquet"
    df.to_parquet(path, index=False, compression="snappy")
    print(f"{name}: {len(df):,} lignes -> {path}")


# 1. Dimension client : une ligne par customer_id.
customers = read_silver("customers")
assert customers["customer_id"].notna().all()
assert customers["customer_id"].is_unique

dim_customer = customers[
    [
        "customer_id", "customer_key", "region", "segment",
        "signup_date", "consent_marketing", "is_active"
    ]
].copy()

dim_customer = dim_customer.sort_values("customer_id").reset_index(drop=True)
dim_customer.insert(0, "customer_sk", range(1, len(dim_customer) + 1))
save_dimension(dim_customer, "dim_customer")


# 2. Dimension produit : une ligne par product_id.
products = read_silver("products")
assert products["product_id"].notna().all()
assert products["product_id"].is_unique

dim_product = products[
    [
        "product_id", "product_key", "sku", "category", "subcategory",
        "brand", "unit_cost", "list_price", "vat_rate", "is_active",
        "effective_from", "effective_to", "is_current"
    ]
].copy()

dim_product = dim_product.sort_values("product_id").reset_index(drop=True)
dim_product.insert(0, "product_sk", range(1, len(dim_product) + 1))
save_dimension(dim_product, "dim_product")


# 3. Dimension magasin : une ligne par store_id.
stores = read_silver("stores")
assert stores["store_id"].notna().all()
assert stores["store_id"].is_unique

dim_store = stores[
    ["store_id", "store_key", "city", "format", "is_active"]
].copy()

dim_store = dim_store.sort_values("store_id").reset_index(drop=True)
dim_store.insert(0, "store_sk", range(1, len(dim_store) + 1))
save_dimension(dim_store, "dim_store")


# 4. Dimension date : calendrier continu entre les dates observées.
date_columns = {
    "orders": "order_ts",
    "pos_sales": "sale_ts",
    "returns": "return_ts",
    "gl_entries": "posting_date",
}

observed_dates = []

for table, column in date_columns.items():
    df = read_silver(table)
    dates = pd.to_datetime(df[column], errors="coerce").dropna()
    if not dates.empty:
        observed_dates.extend(dates.dt.normalize().tolist())

if not observed_dates:
    raise ValueError("Aucune date valide trouvée pour créer dim_date.")

min_date = min(observed_dates)
max_date = max(observed_dates)

calendar = pd.date_range(min_date, max_date, freq="D")
dim_date = pd.DataFrame({"date": calendar})

dim_date["date_key"] = (
    dim_date["date"].dt.strftime("%Y%m%d").astype("int64")
)
dim_date["year"] = dim_date["date"].dt.year
dim_date["quarter"] = dim_date["date"].dt.quarter
dim_date["month"] = dim_date["date"].dt.month
dim_date["month_name"] = dim_date["date"].dt.month_name()
dim_date["day"] = dim_date["date"].dt.day
dim_date["day_of_week"] = dim_date["date"].dt.dayofweek + 1
dim_date["day_name"] = dim_date["date"].dt.day_name()
dim_date["is_weekend"] = dim_date["date"].dt.dayofweek >= 5

dim_date = dim_date[
    [
        "date_key", "date", "year", "quarter", "month", "month_name",
        "day", "day_of_week", "day_name", "is_weekend"
    ]
]

save_dimension(dim_date, "dim_date")


# 5. Rapport de construction.
report = {
    "status": "PASS",
    "dimensions": {
        "dim_customer": len(dim_customer),
        "dim_product": len(dim_product),
        "dim_store": len(dim_store),
        "dim_date": len(dim_date),
    },
    "date_min": str(min_date.date()),
    "date_max": str(max_date.date()),
    "notes": [
        "Une ligne par identifiant métier pour clients, produits et magasins.",
        "Les clés techniques sont générées à partir du tri des identifiants.",
        "Les clés techniques peuvent changer si les données ou le tri changent.",
        "Aucun historique SCD2 n'est reconstruit dans cette version."
    ]
}

report_path = REPORTS / "gold_dimensions_report.json"
report_path.write_text(
    pd.Series(report).to_json(force_ascii=False, indent=2),
    encoding="utf-8"
)

print("\nConstruction des dimensions terminée.")
print(f"Rapport : {report_path}")
