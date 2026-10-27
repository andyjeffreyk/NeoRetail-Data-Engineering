
import json
from pathlib import Path

import pyarrow.parquet as pq


HERE = Path(__file__).resolve()
PROJECT_DIR = next(
    (
        p for p in HERE.parents
        if (p / "data" / "silver").is_dir()
    ),
    None,
)

if PROJECT_DIR is None:
    raise FileNotFoundError("Dossier data/silver introuvable.")

SILVER_DIR = PROJECT_DIR / "data" / "silver"
REPORT_DIR = PROJECT_DIR / "data" / "reports" / "silver"

TABLES = {
    "customers": ("customer_id", 100_000),
    "products": ("product_id", 20_000),
    "stores": ("store_id", 200),
    "orders": ("order_id", 500_000),
    "order_items": ("order_item_id", 1_000_000),
    "pos_sales": ("sale_id", 500_000),
    "returns": ("return_id", 100_000),
    "gl_entries": ("entry_id", 100_000),
}

RELATIONS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("pos_sales", "store_id", "stores", "store_id"),
    ("pos_sales", "product_id", "products", "product_id"),
    ("returns", "order_id", "orders", "order_id"),
    ("returns", "product_id", "products", "product_id"),
]


def read_column(table_name, column_name):
    path = SILVER_DIR / f"{table_name}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Fichier absent : {path}")

    parquet = pq.ParquetFile(path)

    if column_name not in parquet.schema_arrow.names:
        raise ValueError(
            f"Colonne {column_name} absente de {table_name}"
        )

    values = []
    for batch in parquet.iter_batches(
        batch_size=100_000,
        columns=[column_name],
    ):
        values.extend(batch.column(0).to_pylist())

    return values


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results = {
        "tables": [],
        "relationships": [],
        "summary": {},
    }

    print("\nVALIDATION DE LA COUCHE SILVER\n")

    # Vérifier les fichiers, volumes et clés primaires.
    key_sets = {}

    for table, (primary_key, expected_rows) in TABLES.items():
        path = SILVER_DIR / f"{table}.parquet"

        if not path.exists():
            result = {
                "table": table,
                "status": "ERROR",
                "error": "Fichier Parquet absent",
            }
            results["tables"].append(result)
            print(f"ABSENT : {table}.parquet")
            continue

        parquet = pq.ParquetFile(path)
        actual_rows = parquet.metadata.num_rows
        ids = read_column(table, primary_key)

        empty_keys = sum(
            value is None or str(value).strip() == ""
            for value in ids
        )
        valid_ids = [
            str(value) for value in ids
            if value is not None and str(value).strip() != ""
        ]

        unique_ids = set(valid_ids)
        duplicate_rows = len(valid_ids) - len(unique_ids)

        key_sets[table] = unique_ids

        checks_pass = (
            actual_rows == expected_rows
            and empty_keys == 0
            and duplicate_rows == 0
        )

        result = {
            "table": table,
            "status": "PASS" if checks_pass else "REVIEW",
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
            "missing_primary_keys": empty_keys,
            "duplicate_primary_keys": duplicate_rows,
            "columns": parquet.schema_arrow.names,
        }

        results["tables"].append(result)

        print(
            f"{table}: {actual_rows:,} lignes ; "
            f"clés vides={empty_keys:,} ; "
            f"doublons={duplicate_rows:,} ; "
            f"{result['status']}"
        )

    # Vérifier les relations, si les tables sont disponibles.
    for child, child_col, parent, parent_col in RELATIONS:
        if child not in key_sets or parent not in key_sets:
            results["relationships"].append({
                "child": child,
                "column": child_col,
                "parent": parent,
                "status": "NOT_CHECKED",
                "reason": "Table ou clé primaire indisponible",
            })
            continue

        values = read_column(child, child_col)
        parent_ids = key_sets[parent]

        missing = sum(
            value is None or str(value).strip() == ""
            for value in values
        )

        orphans = sum(
            value is not None
            and str(value).strip() != ""
            and str(value) not in parent_ids
            for value in values
        )

        # Les valeurs vides sont mesurées séparément.
        # Elles nécessitent une interprétation métier.
        result = {
            "child": child,
            "column": child_col,
            "parent": parent,
            "parent_column": parent_col,
            "rows_checked": len(values),
            "missing_references": missing,
            "orphan_references": orphans,
            "status": "PASS" if orphans == 0 else "REVIEW",
        }

        results["relationships"].append(result)

        print(
            f"{child}.{child_col} -> {parent}.{parent_col}: "
            f"vides={missing:,}, inconnues={orphans:,}, "
            f"{result['status']}"
        )

    # Le client est facultatif dans une vente POS anonyme.
    pos_path = SILVER_DIR / "pos_sales.parquet"
    if pos_path.exists():
        pos_customers = read_column("pos_sales", "customer_id")
        anonymous_count = sum(
            value is None or str(value).strip() == ""
            for value in pos_customers
        )
        results["pos_sales_customer_policy"] = {
            "anonymous_sales": anonymous_count,
            "policy": "Valeurs vides conservées, à confirmer métier",
        }

        print(
            f"\nVentes POS sans client : {anonymous_count:,}"
        )

    tables_ok = sum(
        item.get("status") == "PASS"
        for item in results["tables"]
    )
    relations_review = sum(
        item.get("status") == "REVIEW"
        for item in results["relationships"]
    )

    results["summary"] = {
        "tables_pass": tables_ok,
        "tables_total": len(TABLES),
        "relationships_review": relations_review,
    }

    report = REPORT_DIR / "silver_validation_report.json"

    with report.open("w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)

    print(f"\nRapport enregistré : {report}")
    print(f"Tables conformes : {tables_ok}/{len(TABLES)}")
    print("Validation terminée.")


if __name__ == "__main__":
    main()
