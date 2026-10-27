
import csv
import gzip
import json
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
REPORT_DIR = PROJECT_DIR / "data" / "reports"

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


def inspect_file(relative_path):
    path = BRONZE_DIR / relative_path

    if not path.exists():
        return {
            "file": relative_path,
            "status": "ERROR",
            "error": "Fichier introuvable",
        }

    row_count = 0
    missing_counts = {}

    with gzip.open(
        path, "rt", encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames or []

        for column in columns:
            missing_counts[column] = 0

        for row in reader:
            row_count += 1

            for column in columns:
                value = row.get(column)

                if value is None or value.strip() == "":
                    missing_counts[column] += 1

    missing_details = []

    for column, count in missing_counts.items():
        if count > 0:
            missing_details.append({
                "column": column,
                "missing_count": count,
                "missing_percent": round(
                    count / row_count * 100, 4
                ) if row_count else 0,
            })

    return {
        "file": relative_path,
        "status": "SUCCESS",
        "rows": row_count,
        "missing_columns": missing_details,
    }


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    print("\nINSPECTION DETAILLEE DES VALEURS MANQUANTES\n")

    for relative_path in FILES:
        print(f"Analyse : {relative_path}")
        result = inspect_file(relative_path)
        results.append(result)

        if result["status"] != "SUCCESS":
            print(f"ERREUR : {result['error']}")
            continue

        if not result["missing_columns"]:
            print("Aucune valeur manquante.")
        else:
            for item in result["missing_columns"]:
                print(
                    f"  {item['column']} : "
                    f"{item['missing_count']:,} valeurs manquantes "
                    f"({item['missing_percent']} %)"
                )

    output = REPORT_DIR / "missing_values_report.json"

    with output.open("w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)

    print(f"\nRapport créé : {output}")

    if any(item["status"] != "SUCCESS" for item in results):
        raise SystemExit(1)

    print("\nInspection terminée.")


if __name__ == "__main__":
    main()
