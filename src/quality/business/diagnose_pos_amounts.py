
from pathlib import Path
import pandas as pd

root = Path.cwd()
path = root / "data" / "silver" / "pos_sales.parquet"

df = pd.read_parquet(
    path,
    columns=["sale_id", "qty", "unit_price", "discount_rate", "net_amount"]
)

# Hypothèse A : calcul sur le total, puis arrondi au centime.
expected_a = (
    df["qty"] * df["unit_price"] * (1 - df["discount_rate"])
).round(2)

# Hypothèse B : prix remisé arrondi au centime, puis multiplié.
discounted_unit = (
    df["unit_price"] * (1 - df["discount_rate"])
).round(2)

expected_b = (df["qty"] * discounted_unit).round(2)

# Hypothèse C : prix unitaire et montant net sont déjà arrondis,
# mais le calcul peut suivre une règle métier différente.
actual = df["net_amount"].round(2)

diff_a = (actual - expected_a).abs().round(2)
diff_b = (actual - expected_b).abs().round(2)

print("\nDIAGNOSTIC DES MONTANTS POS")
print(f"Nombre de ventes : {len(df):,}")

for name, diff in [
    ("Hypothèse A : arrondi en fin de ligne", diff_a),
    ("Hypothèse B : arrondi du prix remisé unitaire", diff_b),
]:
    print(f"\n{name}")
    print(f"Écarts > 0,01 € : {(diff > 0.01).sum():,}")
    print(f"Écarts > 0,03 € : {(diff > 0.03).sum():,}")
    print(f"Écart maximal : {diff.max():.2f} €")

    print("Exemples avec les plus grands écarts :")
    indexes = diff.nlargest(5).index
    result = df.loc[indexes, [
        "sale_id", "qty", "unit_price", "discount_rate", "net_amount"
    ]].copy()
    result["expected_a"] = expected_a.loc[indexes]
    result["expected_b"] = expected_b.loc[indexes]
    result["difference_a"] = diff_a.loc[indexes]
    result["difference_b"] = diff_b.loc[indexes]
    print(result.to_string(index=False))

print("\nRépartition des écarts pour l'hypothèse A :")
print(diff_a.value_counts().sort_index().head(15).to_string())
