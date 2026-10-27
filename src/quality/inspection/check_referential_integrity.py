
import csv
import gzip
import json
from pathlib import Path

# Retrouver automatiquement la racine du projet
HERE = Path(__file__).resolve()
PROJECT_DIR = next(
    (
        parent for parent in HERE.parents
        if (parent / "data" / "bronze").is_dir()
    ),
    None,
)

if PROJECT_DIR is None:
    raise FileNotFoundError(
        "Dossier data/bronze introuvable depuis le script."
    )

RAW_DIR = PROJECT_DIR / "data" / "bronze" / "raw"
REPORT_DIR = PROJECT_DIR / "data" / "reports"

FILES = {
    "customers": "customers/customers.csv.gz",
    "products": "products/products.csv.gz",
    "stores": "stores/stores.csv.gz",
    "orders": "orders/orders.csv.gz",
    "order_items": "orders/order_items.csv.gz",
    "pos_sales": "pos/pos_sales.csv.gz",
    "returns": "returns/returns.csv.gz",
}


def read_ids(file_key, column):
    path = RAW_DIR / FILES[file_key]
    identifiers = set()

    with gzip.open(path, "rt", encoding="utf-8-sig",
                   newline="") as file:
        for row in csv.DictReader(file):
            value = (row.get(column) or "").strip()
            if value:
                identifiers.add(value)

    return identifiers


def check_relation(child, column, parent_ids):
    path = RAW_DIR / FILES[child]
    total = 0
    missing = 0
    orphan = 0

    with gzip.open(path, "rt", encoding="utf-8-sig",
                   newline="") as file:
        for row in csv.DictReader(file):
            total += 1
            value = (row.get(column) or "").strip()

            if not value:
                missing += 1
            elif value not in parent_ids:
                orphan += 1

    return {
        "table": child,
        "column": column,
        "rows_checked": total,
        "missing_references": missing,
        "orphan_references": orphan,
        "status": "PASS" if missing == 0 and orphan == 0
                  else "REVIEW",
    }


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("\nCONTROLE D'INTEGRITE REFERENTIELLE\n")

    # Charger les référentiels une seule fois
    customer_ids = read_ids("customers", "customer_id")
    product_ids = read_ids("products", "product_id")
    store_ids = read_ids("stores", "store_id")
    order_ids = read_ids("orders", "order_id")

    relations = [
        ("orders", "customer_id", customer_ids),
        ("order_items", "order_id", order_ids),
        ("order_items", "product_id", product_ids),
        ("pos_sales", "store_id", store_ids),
        ("pos_sales", "customer_id", customer_ids),
        ("pos_sales", "product_id", product_ids),
        ("returns", "order_id", order_ids),
        ("returns", "product_id", product_ids),
    ]

    results = []

    for child, column, parent_ids in relations:
        result = check_relation(child, column, parent_ids)
        results.append(result)

        print(
            f"{child}.{column}: "
            f"{result['rows_checked']:,} lignes, "
            f"{result['missing_references']:,} références vides, "
            f"{result['orphan_references']:,} références inconnues "
            f"-> {result['status']}"
        )

    output = REPORT_DIR / "referential_integrity_report.json"
    with output.open("w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)

    print(f"\nRapport créé : {output}")
    print("Contrôle terminé.")


if __name__ == "__main__":
    main()
