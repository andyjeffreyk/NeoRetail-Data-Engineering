
import csv
import gzip
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"

FILES = [
    "raw/customers/customers.csv.gz",
    "raw/products/products.csv.gz",
    "raw/stores/stores.csv.gz",
    "raw/orders/orders.csv.gz",
    "raw/orders/order_items.csv.gz",
    "raw/pos/pos_sales.csv.gz",
    "raw/returns/returns.csv.gz",
    "raw/erp/gl_entries.csv.gz",
]

def main():
    print("\nINSPECTION DES COLONNES POUR LES RELATIONS\n")

    for relative_path in FILES:
        path = BRONZE_DIR / relative_path
        print(f"\n--- {relative_path} ---")

        if not path.exists():
            print(f"FICHIER INTROUVABLE : {path}")
            continue

        with gzip.open(
            path, "rt", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.reader(file)
            columns = next(reader, [])

        print(" | ".join(columns))

    print("\nInspection des colonnes terminée.")

if __name__ == "__main__":
    main()
