
import csv
import gzip
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

# Dossier contenant les données sources originales.
SOURCE_DIR = (
    Path.home()
    / "Downloads"
    / "neoretail bigdata pack M2-20261019"
    / "neoretail_bigdata_pack"
)

# Racine du projet : deux niveaux au-dessus de ce script.
PROJECT_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
REPORT_DIR = PROJECT_DIR / "data" / "reports"

# Première ingestion : uniquement les référentiels.
FILES_TO_INGEST = [
    # Référentiels
    Path("raw/customers/customers.csv.gz"),
    Path("raw/products/products.csv.gz"),
    Path("raw/stores/stores.csv.gz"),
    
    # Ventes en ligne
    Path("raw/orders/orders.csv.gz"),
    Path("raw/orders/order_items.csv.gz"),
    
    # Ventes physiques
    Path("raw/pos/pos_sales.csv.gz"),
    
    # Retours et comptabilité
    Path("raw/returns/returns.csv.gz"),
    Path("raw/erp/gl_entries.csv.gz"),
]


def sha256_file(path):
    """Calcule l'empreinte SHA-256 du fichier."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def inspect_csv_gz(path):
    """Compte les lignes et récupère les colonnes du CSV compressé."""
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        columns = next(reader, [])
        row_count = sum(1 for _ in reader)

    return columns, row_count


def main():
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Dossier source introuvable : {SOURCE_DIR}"
        )

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    print(f"Sources : {SOURCE_DIR}")
    print(f"Destination Bronze : {BRONZE_DIR}")
    print()

    for relative_path in FILES_TO_INGEST:
        source = SOURCE_DIR / relative_path
        destination = BRONZE_DIR / relative_path

        print(f"Traitement : {relative_path}")

        if not source.is_file():
            results.append({
                "source": str(relative_path),
                "status": "ERROR",
                "error": "Fichier source introuvable",
            })
            print("  ERREUR : fichier introuvable")
            continue

        try:
            columns, row_count = inspect_csv_gz(source)
            source_hash = sha256_file(source)

            destination.parent.mkdir(parents=True, exist_ok=True)

            # Copie exacte de l'archive, sans modifier la source.
            shutil.copy2(source, destination)

            destination_hash = sha256_file(destination)

            if source_hash != destination_hash:
                raise IOError("L'empreinte de la copie est différente")

            result = {
                "source": str(relative_path),
                "destination": str(destination.relative_to(PROJECT_DIR)),
                "status": "SUCCESS",
                "rows": row_count,
                "columns": columns,
                "column_count": len(columns),
                "size_bytes": source.stat().st_size,
                "sha256": source_hash,
                "ingested_at_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            results.append(result)

            print(f"  Lignes de données : {row_count:,}")
            print(f"  Nombre de colonnes : {len(columns)}")
            print(f"  Copie vérifiée : {destination}")
            print("  Statut : SUCCESS")

        except Exception as error:
            results.append({
                "source": str(relative_path),
                "status": "ERROR",
                "error": str(error),
            })
            print(f"  ERREUR : {error}")

        print()

    report_path = REPORT_DIR / "ingestion_reference_report.json"
    report_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"Rapport : {report_path}")

    if any(result["status"] != "SUCCESS" for result in results):
        raise SystemExit(
            "Ingestion incomplète : consulte le rapport JSON."
        )

    print("Ingestion des référentiels terminée avec succès.")


if __name__ == "__main__":
    main()
