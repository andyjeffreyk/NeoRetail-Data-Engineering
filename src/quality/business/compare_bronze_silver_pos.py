
from pathlib import Path
import pandas as pd

ROOT = Path.cwd()
BRONZE = ROOT / "data" / "bronze" / "raw" / "pos" / "pos_sales.csv.gz"
SILVER = ROOT / "data" / "silver" / "pos_sales.parquet"

if not BRONZE.exists():
    raise FileNotFoundError(f"Source Bronze introuvable : {BRONZE}")

if not SILVER.exists():
    raise FileNotFoundError(f"Fichier Silver introuvable : {SILVER}")

cols = ["sale_id", "qty", "unit_price", "discount_rate", "net_amount"]

print("Lecture de la source Bronze...")
bronze = pd.read_csv(BRONZE, compression="gzip", usecols=cols, dtype={"sale_id": "string"})

print("Lecture de Silver...")
silver = pd.read_parquet(SILVER, columns=cols)
silver["sale_id"] = silver["sale_id"].astype("string")

print("\nVolumes")
print(f"Bronze : {len(bronze):,}")
print(f"Silver : {len(silver):,}")

merged = bronze.merge(
    silver,
    on="sale_id",
    how="outer",
    suffixes=("_bronze", "_silver"),
    indicator=True,
    validate="one_to_one",
)

print("\nCorrespondance des clés")
print(merged["_merge"].value_counts().to_string())

matched = merged[merged["_merge"] == "both"].copy()

for col in ["qty", "unit_price", "discount_rate", "net_amount"]:
    left = pd.to_numeric(matched[f"{col}_bronze"], errors="coerce")
    right = pd.to_numeric(matched[f"{col}_silver"], errors="coerce")
    diff = (left - right).abs()
    print(
        f"{col}: écarts > 0,000001 = {(diff > 0.000001).sum():,} ; "
        f"écart maximal = {diff.max()}"
    )

# Calculs à partir des valeurs Silver
qty = pd.to_numeric(matched["qty_silver"], errors="coerce")
price = pd.to_numeric(matched["unit_price_silver"], errors="coerce")
discount = pd.to_numeric(matched["discount_rate_silver"], errors="coerce")
actual = pd.to_numeric(matched["net_amount_silver"], errors="coerce")

expected = (qty * price * (1 - discount)).round(2)
diff = (actual - expected).abs().round(2)

print("\nFormule calculée depuis Silver")
print(f"Écarts > 0,03 € : {(diff > 0.03).sum():,}")
print(f"Écart maximal : {diff.max():.2f} €")

print("\nExemples d'écarts supérieurs à 0,03 €")
bad = matched.loc[diff > 0.03, [
    "sale_id",
    "qty_bronze", "qty_silver",
    "unit_price_bronze", "unit_price_silver",
    "discount_rate_bronze", "discount_rate_silver",
    "net_amount_bronze", "net_amount_silver",
]].copy()

bad["expected_silver"] = expected[diff > 0.03]
bad["difference"] = diff[diff > 0.03]
print(bad.head(15).to_string(index=False))

output = ROOT / "data" / "reports" / "silver" / "pos_bronze_silver_comparison.csv"
bad.to_csv(output, index=False, encoding="utf-8-sig")
print(f"\nExemples enregistrés dans : {output}")
