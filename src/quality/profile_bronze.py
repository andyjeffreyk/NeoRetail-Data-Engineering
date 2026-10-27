
import csv
import gzip
import json
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone

PROJECT_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
REPORT_DIR = PROJECT_DIR / "data" / "reports"

# Clés candidates à contrôler d'après l'inventaire fourni.
KEYS = {
    "raw/customers/customers.csv.gz": ["customer_id"],
    "raw/products/products.csv.gz": ["product_id"],
    "raw/stores/stores.csv.gz": ["store_id"],
    "raw/orders/orders.csv.gz": ["order_id"],
    "raw/orders/order_items.csv.gz": ["order_item_id"],
    "raw/pos/pos_sales.csv.gz": ["sale_id"],
    "raw/returns/returns.csv.gz": ["return_id"],
    "raw/erp/gl_entries.csv.gz": ["entry_id"],
}

def profile_file(relative_path, key_columns):
    path = BRONZE_DIR / relative_path

    if not path.exists():
        return {
            "file": relative_path,
            "status": "ERROR",
            "error": "Fichier Bronze introuvable",
        }

    row_count = 0
    null_counts = Counter()
    key_counts = Counter()
    exact_rows = set()
    duplicate_exact_count = 0
    columns = []

    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []

        missing_keys = [k for k in key_columns if k not in columns]
        if missing_keys:
            return {
                "file": relative_path,
                "status": "ERROR",
                "error": f"Colonnes de clé absentes : {missing_keys}",
                "columns": columns,
            }

        for row in reader:
            row_count += 1

            for column, value in row.items():
                if value is None or value.strip() == "":
                    null_counts[column] += 1

            key = tuple((row.get(k) or "").strip() for k in key_columns)
            if any(value == "" for value in key):
                key_counts["missing_key"] += 1
            else:
                key_counts[key] += 1

            # Empreinte logique de la ligne pour détecter les doublons exacts.
            serialized = json.dumps(
                row, sort_keys=True, ensure_ascii=False
            )
            if serialized in exact_rows:
                duplicate_exact_count += 1
            else:
                exact_rows.add(serialized)

    duplicate_keys = sum(
        count - 1
        for key, count in key_counts.items()
        if key != "missing_key" and count > 1
    )

    return {
        "file": relative_path,
        "status": "SUCCESS",
        "rows": row_count,
        "columns": columns,
        "column_count": len(columns),
        "null_counts": dict(null_counts),
        "missing_key_rows": key_counts["missing_key"],
        "duplicate_key_rows": duplicate_keys,
        "duplicate_exact_rows": duplicate_exact_count,
        "checked_key_columns": key_columns,
    }


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    print("PROFILAGE QUALITÉ — BRONZE\n")

    for relative_path, key_columns in KEYS.items():
        print(f"Analyse : {relative_path}")
        result = profile_file(relative_path, key_columns)
        results.append(result)

        if result["status"] != "SUCCESS":
            print(f"  ERREUR : {result['error']}")
            continue

        print(f"  Lignes : {result['rows']:,}")
        print(f"  Clés manquantes : {result['missing_key_rows']:,}")
        print(f"  Doublons de clé : {result['duplicate_key_rows']:,}")
        print(f"  Doublons exacts : {result['duplicate_exact_rows']:,}")
        print(f"  Colonnes avec valeurs manquantes : "
              f"{len(result['null_counts'])}")
        print()

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_layer": "Bronze",
        "results": results,
    }

    json_path = REPORT_DIR / "bronze_quality_report.json"
    csv_path = REPORT_DIR / "bronze_quality_summary.csv"

    json_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        fields = [
            "file",
            "status",
            "rows",
            "column_count",
            "missing_key_rows",
            "duplicate_key_rows",
            "duplicate_exact_rows",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for result in results:
            writer.writerow({
                field: result.get(field, "")
                for field in fields
            })

    print("Rapports créés :")
    print(f"- {json_path}")
    print(f"- {csv_path}")

    if any(result["status"] != "SUCCESS" for result in results):
        raise SystemExit("Certains fichiers n'ont pas pu être analysés.")

    print("\nProfilage terminé.")


if __name__ == "__main__":
    main()
