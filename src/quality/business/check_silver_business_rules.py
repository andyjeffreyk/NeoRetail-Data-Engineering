
from pathlib import Path
import json
from datetime import datetime, timezone

import pandas as pd

ROOT = Path.cwd()
SILVER = ROOT / "data" / "silver"
REPORT_DIR = ROOT / "data" / "reports" / "silver"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

results = []


def add_check(table, rule, status, count, examples=None, details=""):
    results.append({
        "table": table,
        "rule": rule,
        "status": status,
        "anomaly_count": int(count),
        "examples": examples or [],
        "details": details,
    })


def load_table(name):
    path = SILVER / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    return pd.read_parquet(path)


def check_numeric(df, table, column, rule, invalid_mask, details):
    mask = invalid_mask.fillna(False)
    add_check(
        table,
        rule,
        "WARNING" if mask.any() else "PASS",
        mask.sum(),
        df.loc[mask, column].head(5).tolist(),
        details,
    )


def check_allowed(df, table, column, allowed):
    invalid = df[column].notna() & ~df[column].isin(allowed)
    add_check(
        table,
        f"{column}_allowed_values",
        "WARNING" if invalid.any() else "PASS",
        invalid.sum(),
        df.loc[invalid, column].head(5).tolist(),
        f"Valeurs autorisées : {sorted(allowed)}",
    )


tables = [
    "customers", "products", "stores", "orders",
    "order_items", "pos_sales", "returns", "gl_entries",
]

data = {}

for table in tables:
    try:
        data[table] = load_table(table)
        print(f"Table chargée : {table} ({len(data[table]):,} lignes)")
    except Exception as exc:
        add_check(table, "table_readable", "WARNING", 1, details=str(exc))

# 1. Produits
if "products" in data:
    df = data["products"]

    check_numeric(
        df, "products", "unit_cost", "unit_cost_non_negative",
        df["unit_cost"] < 0, "Le coût unitaire ne devrait pas être négatif."
    )
    check_numeric(
        df, "products", "list_price", "list_price_positive",
        df["list_price"] <= 0, "Le prix catalogue devrait être strictement positif."
    )
    check_numeric(
        df, "products", "vat_rate", "vat_rate_between_0_and_1",
        (df["vat_rate"] < 0) | (df["vat_rate"] > 1),
        "Le taux de TVA est attendu sous forme décimale, par exemple 0.20 pour 20 %."
    )

    start = pd.to_datetime(df["effective_from"], errors="coerce")
    end = pd.to_datetime(df["effective_to"], errors="coerce")
    invalid_dates = start.isna() | (end.notna() & (end < start))
    add_check(
        "products", "validity_dates", "WARNING" if invalid_dates.any() else "PASS",
        invalid_dates.sum(),
        df.loc[invalid_dates, ["product_id", "effective_from", "effective_to"]]
          .head(5).to_dict("records"),
        "effective_to peut être nul pour une version sans date de fin."
    )

    current = df["is_current"].fillna(False).astype(bool)
    inconsistent = (current & end.notna()) | (~current & end.isna())
    add_check(
        "products", "current_version_consistency",
        "REVIEW" if inconsistent.any() else "PASS",
        inconsistent.sum(),
        df.loc[inconsistent, ["product_id", "effective_to", "is_current"]]
          .head(5).to_dict("records"),
        "À confirmer selon la règle de gestion des versions produits."
    )

# 2. Commandes
if "orders" in data:
    df = data["orders"]
    check_allowed(df, "orders", "channel", {"marketplace", "mobile", "web"})
    check_allowed(
        df, "orders", "payment_method",
        {"card", "gift_card", "wallet", "transfer"}
    )
    check_allowed(
        df, "orders", "status",
        {"paid", "delivered", "shipped", "cancelled"}
    )

    dates = pd.to_datetime(df["order_ts"], errors="coerce")
    invalid = dates.isna()
    add_check(
        "orders", "valid_order_timestamp", "WARNING" if invalid.any() else "PASS",
        invalid.sum(), df.loc[invalid, ["order_id", "order_ts"]]
        .head(5).to_dict("records"),
        "La date/heure de commande doit être interprétable."
    )

# 3. Lignes de commande et ventes POS
for table in ["order_items", "pos_sales"]:
    if table not in data:
        continue

    df = data[table]
    for col in ["qty", "unit_price", "net_amount"]:
        invalid = df[col].isna() | (df[col] <= 0)
        check_numeric(
            df, table, col, f"{col}_positive", invalid,
            "La valeur doit être présente et strictement positive."
        )

    discount = df["discount_rate"]
    invalid_discount = discount.isna() | (discount < 0) | (discount > 1)
    check_numeric(
        df, table, "discount_rate", "discount_between_0_and_1",
        invalid_discount,
        "Le taux de remise doit être compris entre 0 et 1."
    )

    
    # Hypothèse à vérifier : montant net = quantité × prix × (1 - remise).
    # Cette formule n'est pas encore confirmée par le contrat métier.
    expected = (df["qty"] * df["unit_price"] * (1 - discount)).round(2)
    actual = df["net_amount"].round(2)
    difference = (actual - expected).abs().round(2)

    # On évite les faux écarts dus aux flottants et aux arrondis.
    invalid_amount = difference > 0.03

    add_check(
        table,
        "net_amount_formula_review",
        "REVIEW" if invalid_amount.any() else "PASS",
        invalid_amount.sum(),
        df.loc[invalid_amount, [
            "qty", "unit_price", "discount_rate", "net_amount"
        ]].head(5).to_dict("records"),
        "Contrôle exploratoire uniquement : formule supposée, non confirmée "
        "par une règle métier officielle. Les montants sources sont conservés. "
        "Les écarts dépassant 0,03 € après arrondi sont à investiguer."
    )

# 4. Retours
if "returns" in data:
    df = data["returns"]

    for col in ["return_qty", "return_amount"]:
        invalid = df[col].isna() | (df[col] <= 0)
        check_numeric(
            df, "returns", col, f"{col}_positive", invalid,
            "La quantité et le montant du retour sont attendus positifs."
        )

    check_allowed(
        df, "returns", "reason",
        {"late", "wrong_item", "damaged", "size_issue",
         "other", "not_as_expected"}
    )

    dates = pd.to_datetime(df["return_ts"], errors="coerce")
    invalid = dates.isna()
    add_check(
        "returns", "valid_return_timestamp",
        "WARNING" if invalid.any() else "PASS",
        invalid.sum(),
        df.loc[invalid, ["return_id", "return_ts"]]
        .head(5).to_dict("records"),
        "La date/heure du retour doit être interprétable."
    )

# 5. Comptabilité
if "gl_entries" in data:
    df = data["gl_entries"]

    check_allowed(
        df, "gl_entries", "currency", {"EUR"}
    )
    check_allowed(
        df, "gl_entries", "document_type",
        {"PAYMENT", "ADJUSTMENT", "REFUND", "SALE", "PURCHASE"}
    )

    dates = pd.to_datetime(df["posting_date"], errors="coerce")
    invalid = dates.isna()
    add_check(
        "gl_entries", "valid_posting_date",
        "WARNING" if invalid.any() else "PASS",
        invalid.sum(),
        df.loc[invalid, ["entry_id", "posting_date"]]
        .head(5).to_dict("records"),
        "La date comptable doit être interprétable."
    )

# 6. Clients et magasins
if "customers" in data:
    df = data["customers"]

    missing_region = df["region"].isna()
    add_check(
        "customers", "missing_region",
        "WARNING" if missing_region.any() else "PASS",
        missing_region.sum(),
        df.loc[missing_region, ["customer_id"]].head(5).to_dict("records"),
        "Les régions manquantes sont signalées, pas inventées."
    )

    for col in ["consent_marketing", "is_active"]:
        invalid = ~df[col].isin([True, False])
        add_check(
            "customers", f"{col}_boolean",
            "WARNING" if invalid.any() else "PASS",
            invalid.sum(), df.loc[invalid, [col]].head(5).to_dict("records"),
            "Valeurs attendues : True ou False."
        )

if "stores" in data:
    df = data["stores"]
    check_allowed(
        df, "stores", "format",
        {"super", "outlet", "hyper", "proximity"}
    )

    invalid = ~df["is_active"].isin([True, False])
    add_check(
        "stores", "is_active_boolean",
        "WARNING" if invalid.any() else "PASS",
        invalid.sum(), df.loc[invalid, ["is_active"]].head(5).to_dict("records"),
        "Valeurs attendues : True ou False."
    )

# 7. Ventes POS anonymes : information, pas échec automatique.
if "pos_sales" in data:
    df = data["pos_sales"]
    anonymous = df["customer_id"].isna()
    add_check(
        "pos_sales", "anonymous_sales",
        "REVIEW" if anonymous.any() else "PASS",
        anonymous.sum(),
        df.loc[anonymous, ["sale_id"]].head(5).to_dict("records"),
        "Les ventes sans client identifié sont conservées ; vérifier "
        "qu'elles correspondent bien au fonctionnement métier."
    )

# 8. Résumé et rapport
counts = {
    status: sum(r["status"] == status for r in results)
    for status in ["PASS", "WARNING", "REVIEW"]
}

report = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "silver_path": str(SILVER),
    "checks_count": len(results),
    "summary": counts,
    "results": results,
}

output = REPORT_DIR / "silver_business_rules_report.json"
output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                  encoding="utf-8")

print("\nVALIDATION MÉTIER SILVER")
print(f"Contrôles : {len(results)}")
for status, count in counts.items():
    print(f"{status}: {count}")
print(f"\nRapport enregistré : {output}")
