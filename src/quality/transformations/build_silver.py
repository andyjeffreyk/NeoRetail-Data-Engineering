
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Retrouver la racine du projet sans dépendre du nom du dossier parent.
HERE = Path(__file__).resolve()
PROJECT_DIR = next(
    (
        parent for parent in HERE.parents
        if (parent / "data" / "bronze" / "raw").is_dir()
    ),
    None,
)

if PROJECT_DIR is None:
    raise FileNotFoundError(
        "Impossible de trouver data/bronze/raw depuis le script."
    )

RAW_DIR = PROJECT_DIR / "data" / "bronze" / "raw"
SILVER_DIR = PROJECT_DIR / "data" / "silver"
REPORT_DIR = PROJECT_DIR / "data" / "reports" / "silver"
CHUNK_SIZE = 100_000


# Fichiers sources et clés primaires attendues.
TABLES = {
    "customers": ("customers/customers.csv.gz", "customer_id"),
    "products": ("products/products.csv.gz", "product_id"),
    "stores": ("stores/stores.csv.gz", "store_id"),
    "orders": ("orders/orders.csv.gz", "order_id"),
    "order_items": ("orders/order_items.csv.gz", "order_item_id"),
    "pos_sales": ("pos/pos_sales.csv.gz", "sale_id"),
    "returns": ("returns/returns.csv.gz", "return_id"),
    "gl_entries": ("erp/gl_entries.csv.gz", "entry_id"),
}

NUMERIC_COLUMNS = {
    "unit_cost", "list_price", "vat_rate",
    "qty", "unit_price", "discount_rate", "net_amount",
    "return_qty", "return_amount", "amount",
}

DATETIME_COLUMNS = {
    "signup_date", "effective_from", "effective_to",
    "order_ts", "sale_ts", "return_ts", "posting_date",
}

BOOLEAN_COLUMNS = {
    "consent_marketing", "is_active", "is_current",
}


def convert_chunk(df):
    """Convertit les types sans supprimer les lignes."""

    errors = {}
    missing_before = {
        col: int(df[col].isna().sum() + df[col].eq("").sum())
        for col in df.columns
    }

    # Les champs vides deviennent des valeurs nulles.
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    for col in df.columns:
        if col in NUMERIC_COLUMNS:
            original = df[col].copy()
            converted = pd.to_numeric(original, errors="coerce")

            errors[col] = int(
                (original.notna() & converted.isna()).sum()
            )
            df[col] = converted.astype("float64")

        elif col in DATETIME_COLUMNS:
            original = df[col].copy()
            converted = pd.to_datetime(
                original, errors="coerce", utc=True
            )
            errors[col] = int(
                (original.notna() & converted.isna()).sum()
            )
            df[col] = converted

        elif col in BOOLEAN_COLUMNS:
            original = df[col].astype("string").str.strip().str.lower()

            mapping = {
                "true": True, "false": False,
                "1": True, "0": False,
                "yes": True, "no": False,
                "1.0": True, "0.0": False,
            }

            converted = original.map(mapping).astype("boolean")
            errors[col] = int(
                (original.notna() & converted.isna()).sum()
            )
            df[col] = converted

        else:
            # Les identifiants et les autres champs restent textuels.
            df[col] = df[col].astype("string")

    return df, missing_before, errors


def transform_table(table_name, source_file, primary_key):
    source = RAW_DIR / source_file
    destination = SILVER_DIR / f"{table_name}.parquet"

    if not source.exists():
        raise FileNotFoundError(f"Source introuvable : {source}")

    if destination.exists():
        destination.unlink()

    total_rows = 0
    missing_counts = {}
    conversion_errors = {}
    key_missing = 0
    writer = None
    expected_columns = None

    try:
        chunks = pd.read_csv(
            source,
            compression="gzip",
            dtype=str,
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            encoding="utf-8-sig",
        )

        for chunk_number, chunk in enumerate(chunks, start=1):
            if expected_columns is None:
                expected_columns = list(chunk.columns)

                if primary_key not in expected_columns:
                    raise ValueError(
                        f"{table_name}: clé absente : {primary_key}"
                    )

            elif list(chunk.columns) != expected_columns:
                raise ValueError(
                    f"{table_name}: schéma différent entre les lots."
                )

            chunk, missing, errors = convert_chunk(chunk)

            for col, count in missing.items():
                missing_counts[col] = (
                    missing_counts.get(col, 0) + count
                )

            for col, count in errors.items():
                conversion_errors[col] = (
                    conversion_errors.get(col, 0) + count
                )

            key_missing += int(chunk[primary_key].isna().sum())
            total_rows += len(chunk)

            table = pa.Table.from_pandas(
                chunk, preserve_index=False
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    destination, table.schema, compression="snappy"
                )
            else:
                # Toute différence de schéma doit être détectée.
                table = table.cast(writer.schema, safe=False)

            writer.write_table(table)

            print(
                f"{table_name}: lot {chunk_number} écrit "
                f"({total_rows:,} lignes cumulées)"
            )

    finally:
        if writer is not None:
            writer.close()

    if total_rows == 0:
        raise ValueError(f"La source {table_name} ne contient aucune ligne.")

    # Vérification de lecture du fichier produit.
    parquet_file = pq.ParquetFile(destination)
    silver_rows = parquet_file.metadata.num_rows

    if silver_rows != total_rows:
        raise RuntimeError(
            f"{table_name}: {total_rows} lignes lues, "
            f"mais {silver_rows} lignes dans Silver."
        )

    return {
        "table": table_name,
        "source": str(source.relative_to(PROJECT_DIR)),
        "output": str(destination.relative_to(PROJECT_DIR)),
        "status": "SUCCESS",
        "source_rows": total_rows,
        "silver_rows": silver_rows,
        "primary_key": primary_key,
        "missing_primary_key": key_missing,
        "missing_values_by_column": missing_counts,
        "conversion_errors_by_column": conversion_errors,
        "columns": expected_columns,
        "parquet_compression": "snappy",
    }


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    print("\nCONSTRUCTION DE LA COUCHE SILVER\n")

    for table_name, (source_file, primary_key) in TABLES.items():
        print(f"\n--- {table_name} ---")

        try:
            result = transform_table(
                table_name, source_file, primary_key
            )
            results.append(result)

            print(f"Statut : {result['status']}")
            print(f"Lignes Bronze lues : {result['source_rows']:,}")
            print(f"Lignes Silver écrites : {result['silver_rows']:,}")
            print(
                "Clés primaires vides après conversion : "
                f"{result['missing_primary_key']:,}"
            )

            if any(result["conversion_errors_by_column"].values()):
                print(
                    "ATTENTION : des erreurs de conversion sont "
                    "présentes dans le rapport."
                )

        except Exception as exc:
            results.append({
                "table": table_name,
                "status": "ERROR",
                "error": str(exc),
            })
            print(f"ERREUR : {exc}")

    report_path = REPORT_DIR / "silver_build_report.json"

    with report_path.open("w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)

    success = sum(r["status"] == "SUCCESS" for r in results)
    failures = len(results) - success

    print("\n" + "=" * 50)
    print(f"Tables réussies : {success}/{len(TABLES)}")
    print(f"Tables en erreur : {failures}")
    print(f"Rapport : {report_path}")
    print(f"Dossier Silver : {SILVER_DIR}")

    if failures:
        raise SystemExit(1)

    print("\nConstruction Silver terminée.")


if __name__ == "__main__":
    main()
