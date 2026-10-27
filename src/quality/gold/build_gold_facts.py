
from pathlib import Path
import json
import pandas as pd

ROOT = Path.cwd()
SILVER = ROOT / "data" / "silver"
DIMENSIONS = ROOT / "data" / "gold" / "dimensions"
FACTS = ROOT / "data" / "gold" / "facts"
REPORTS = ROOT / "data" / "reports" / "gold"

FACTS.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

report = {
    "status": "RUNNING",
    "facts": {},
    "errors": [],
}


def read_parquet(folder: Path, name: str) -> pd.DataFrame:
    path = folder / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    return pd.read_parquet(path)


def save_fact(df: pd.DataFrame, name: str, key: str) -> None:
    if key not in df.columns:
        raise ValueError(f"{name}: clé absente : {key}")

    if df[key].isna().any():
        raise ValueError(f"{name}: valeurs nulles dans {key}")

    if df[key].duplicated().any():
        raise ValueError(f"{name}: doublons dans {key}")

    path = FACTS / f"{name}.parquet"
    df.to_parquet(path, index=False, compression="snappy")

    report["facts"][name] = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "primary_key": key,
        "primary_key_nulls": int(df[key].isna().sum()),
        "primary_key_duplicates": int(df[key].duplicated().sum()),
        "file": str(path.relative_to(ROOT)),
    }


def add_surrogate_key(
    fact: pd.DataFrame,
    dimension: pd.DataFrame,
    natural_key: str,
    surrogate_key: str,
) -> pd.DataFrame:
    lookup = dimension[[natural_key, surrogate_key]].copy()

    if lookup[natural_key].isna().any():
        raise ValueError(f"Clé naturelle nulle dans la dimension : {natural_key}")

    if lookup[natural_key].duplicated().any():
        raise ValueError(f"Clé naturelle non unique dans la dimension : {natural_key}")

    if natural_key not in fact.columns:
        raise ValueError(f"Clé absente de la table de faits : {natural_key}")

    result = fact.merge(
        lookup,
        on=natural_key,
        how="left",
        validate="many_to_one",
        indicator=True,
    )

    unmatched = int((result["_merge"] != "both").sum())
    result = result.drop(columns=["_merge"])

    if unmatched:
        raise ValueError(
            f"{unmatched} lignes de faits sans correspondance "
            f"dans la dimension pour {natural_key}"
        )

    if result[surrogate_key].isna().any():
        raise ValueError(f"Clés techniques nulles après jointure : {surrogate_key}")

    return result


# Charger les dimensions Gold
dim_customer = read_parquet(DIMENSIONS, "dim_customer")
dim_product = read_parquet(DIMENSIONS, "dim_product")
dim_store = read_parquet(DIMENSIONS, "dim_store")
dim_date = read_parquet(DIMENSIONS, "dim_date")

# Charger les données Silver
orders = read_parquet(SILVER, "orders")
order_items = read_parquet(SILVER, "order_items")
pos_sales = read_parquet(SILVER, "pos_sales")
returns = read_parquet(SILVER, "returns")
gl_entries = read_parquet(SILVER, "gl_entries")

# ---------------------------------------------------------
# 1. FACT_ORDER_ITEMS : une ligne par article de commande
# ---------------------------------------------------------
fact_order_items = order_items.merge(
    orders[["order_id", "order_ts", "customer_id", "channel",
            "payment_method", "status", "promo_code"]],
    on="order_id",
    how="left",
    validate="many_to_one",
    indicator=True,
)

if (fact_order_items["_merge"] != "both").any():
    raise ValueError("Des articles de commande ne correspondent à aucune commande.")

fact_order_items = fact_order_items.drop(columns=["_merge"])

fact_order_items = add_surrogate_key(
    fact_order_items, dim_product, "product_id", "product_sk"
)

fact_order_items = add_surrogate_key(
    fact_order_items, dim_customer, "customer_id", "customer_sk"
)

fact_order_items["order_date"] = pd.to_datetime(
    fact_order_items["order_ts"], errors="raise"
).dt.normalize()

date_lookup = dim_date[["date", "date_key"]].copy()
date_lookup["date"] = pd.to_datetime(date_lookup["date"]).dt.normalize()

fact_order_items = fact_order_items.merge(
    date_lookup.rename(columns={"date_key": "date_sk"}),
    left_on="order_date",
    right_on="date",
    how="left",
    validate="many_to_one",
    indicator=True,
)

if (fact_order_items["_merge"] != "both").any():
    raise ValueError("Certaines dates de commandes sont absentes de dim_date.")

fact_order_items = fact_order_items.drop(columns=["_merge", "date"])
fact_order_items = fact_order_items.rename(columns={"order_date": "order_date_value"})

save_fact(fact_order_items, "fact_order_items", "order_item_id")

# ---------------------------------------------------------
# 2. FACT_POS_SALES : une ligne par vente POS
# ---------------------------------------------------------
fact_pos_sales = pos_sales.copy()

fact_pos_sales = add_surrogate_key(
    fact_pos_sales, dim_product, "product_id", "product_sk"
)

fact_pos_sales = add_surrogate_key(
    fact_pos_sales, dim_store, "store_id", "store_sk"
)

# Les ventes anonymes sont conservées : customer_sk reste nul.
customer_lookup = dim_customer[["customer_id", "customer_sk"]].copy()

fact_pos_sales = fact_pos_sales.merge(
    customer_lookup,
    on="customer_id",
    how="left",
    validate="many_to_one",
)

fact_pos_sales["sale_date"] = pd.to_datetime(
    fact_pos_sales["sale_ts"], errors="raise"
).dt.normalize()

fact_pos_sales = fact_pos_sales.merge(
    date_lookup.rename(columns={"date_key": "date_sk"}),
    left_on="sale_date",
    right_on="date",
    how="left",
    validate="many_to_one",
    indicator=True,
)

if (fact_pos_sales["_merge"] != "both").any():
    raise ValueError("Certaines dates de ventes POS sont absentes de dim_date.")

fact_pos_sales = fact_pos_sales.drop(columns=["_merge", "date"])
fact_pos_sales = fact_pos_sales.rename(columns={"sale_date": "sale_date_value"})

save_fact(fact_pos_sales, "fact_pos_sales", "sale_id")

# ---------------------------------------------------------
# 3. FACT_RETURNS : une ligne par retour
# ---------------------------------------------------------
fact_returns = returns.copy()

fact_returns = add_surrogate_key(
    fact_returns, dim_product, "product_id", "product_sk"
)

fact_returns["return_date"] = pd.to_datetime(
    fact_returns["return_ts"], errors="raise"
).dt.normalize()

fact_returns = fact_returns.merge(
    date_lookup.rename(columns={"date_key": "date_sk"}),
    left_on="return_date",
    right_on="date",
    how="left",
    validate="many_to_one",
    indicator=True,
)

if (fact_returns["_merge"] != "both").any():
    raise ValueError("Certaines dates de retours sont absentes de dim_date.")

fact_returns = fact_returns.drop(columns=["_merge", "date"])
fact_returns = fact_returns.rename(columns={"return_date": "return_date_value"})

save_fact(fact_returns, "fact_returns", "return_id")

# ---------------------------------------------------------
# 4. FACT_GL_ENTRIES : une ligne par écriture comptable
# ---------------------------------------------------------
fact_gl_entries = gl_entries.copy()

fact_gl_entries["posting_date_value"] = pd.to_datetime(
    fact_gl_entries["posting_date"], errors="raise"
).dt.normalize()

fact_gl_entries = fact_gl_entries.merge(
    date_lookup.rename(columns={"date_key": "date_sk"}),
    left_on="posting_date_value",
    right_on="date",
    how="left",
    validate="many_to_one",
    indicator=True,
)

if (fact_gl_entries["_merge"] != "both").any():
    raise ValueError("Certaines dates comptables sont absentes de dim_date.")

fact_gl_entries = fact_gl_entries.drop(columns=["_merge", "date"])

save_fact(fact_gl_entries, "fact_gl_entries", "entry_id")

# Rapport final
report["status"] = "PASS"
report["message"] = "Les quatre tables de faits Gold ont été créées et contrôlées."

report_path = REPORTS / "gold_facts_report.json"
report_path.write_text(
    json.dumps(report, ensure_ascii=False, indent=2),
    encoding="utf-8",
)

print("CONSTRUCTION DES FAITS GOLD TERMINÉE")
print(f"Statut : {report['status']}")

for name, info in report["facts"].items():
    print(
        f"{name}: {info['rows']:,} lignes | "
        f"clé={info['primary_key']} | "
        f"colonnes={len(info['columns'])}"
    )

print(f"Rapport : {report_path}")
