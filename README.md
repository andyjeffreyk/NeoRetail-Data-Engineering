# NeoRetail — Plateforme Big Data, Data Warehouse et qualité des données

## 1. Présentation du projet

NeoRetail est un projet de plateforme de données appliquée au secteur du commerce de détail. Il vise à construire progressivement une chaîne de traitement permettant d'ingérer des données, de contrôler leur qualité, de les transformer et de préparer des données exploitables pour l'analyse décisionnelle.

Le projet s'appuie sur un jeu de données comprenant notamment les clients, les produits, les magasins, les commandes, les lignes de commande, les ventes en point de vente, les retours et les écritures comptables.

**État actuel :** les principales étapes de préparation Bronze, de transformation Silver, de contrôle qualité et de construction des données Gold ont été réalisées. L'orchestration complète et les autres composants de la plateforme restent à intégrer ou à vérifier.

## 2. Objectifs

- Organiser les données selon une architecture Bronze, Silver et Gold.
- Contrôler la qualité, la cohérence et l'intégrité référentielle des données.
- Documenter les anomalies sans modifier arbitrairement les données sources.
- Construire des dimensions et des tables de faits adaptées à l'analyse.
- Mettre progressivement en place l'orchestration, le suivi des traitements et le suivi des expériences de machine learning.
- Préparer une plateforme démontrable de bout en bout.

## 3. Architecture des données

### Bronze — Données brutes

La couche Bronze correspond aux données ingérées depuis le jeu de données source. Les contrôles et rapports disponibles permettent notamment d'examiner les valeurs manquantes, les clés, les doublons et certaines relations entre tables.

### Silver — Données transformées et contrôlées

La couche Silver contient les tables préparées pour les traitements analytiques. Des scripts assurent la construction, la validation et l'examen des règles métier.

### Gold — Données analytiques

La couche Gold contient des dimensions et des tables de faits préparées pour les usages analytiques.

**Dimensions identifiées :**
- `dim_customer`
- `dim_product`
- `dim_store`
- `dim_date`

**Tables de faits identifiées :**
- `fact_order_items`
- `fact_pos_sales`
- `fact_returns`
- `fact_gl_entries`

Les chemins, schémas et volumes exacts doivent être vérifiés à partir des fichiers Parquet et des rapports générés.

## 4. Contrôles qualité

Les contrôles disponibles portent notamment sur :
- les valeurs manquantes ;
- les clés et les doublons ;
- l'intégrité référentielle ;
- la validation de la couche Silver ;
- les règles métier ;
- la comparaison des montants entre Bronze et Silver.

Certaines anomalies ont été identifiées et doivent rester documentées jusqu'à ce que leur interprétation métier soit confirmée. Par exemple, des ventes en point de vente ne possèdent pas de `customer_id`, et certaines lignes présentent un écart entre le montant calculé et le montant observé. Ces situations ne doivent pas être corrigées arbitrairement.

## 5. Structure actuelle du dépôt

```text
NeoRetail_Project/
├── README.md
├── data/
│   └── reports/
│       ├── bronze_quality_report.json
│       ├── bronze_quality_summary.csv
│       ├── ingestion_reference_report.json
│       ├── missing_values_report.json
│       ├── referential_integrity_report.json
│       ├── silver/
│       └── gold/
├── docs/
│   └── architecture/
│       └── L1_Dossier_Architecture_NeoRetail.md
├── infra/
│   ├── docker/
│   │   └── compose.yaml
│   └── mlflow/
│       └── README.md
└── src/
    ├── ingestion/
    │   └── ingest_reference_data.py
    └── quality/
        ├── profile_bronze.py
        ├── inspect_missing_values.py
        ├── inspection_relationship_columns.py
        ├── inspection/
        ├── transformations/
        └── business/
```

Cette arborescence présente les principaux fichiers identifiés. Elle ne constitue pas une liste exhaustive de tous les fichiers du projet.

## 6. Scripts et rapports disponibles

| Domaine | Fichier ou emplacement | Rôle |
|---|---|---|
| Ingestion | `src/ingestion/ingest_reference_data.py` | Ingestion des données de référence |
| Profilage Bronze | `src/quality/profile_bronze.py` | Profilage de la qualité des données brutes |
| Valeurs manquantes | `src/quality/inspect_missing_values.py` | Inspection des valeurs manquantes |
| Intégrité référentielle | `src/quality/inspection/check_referential_integrity.py` | Vérification de relations entre tables |
| Validation Silver | `src/quality/transformations/validate_silver.py` | Validation des données transformées |
| Règles métier | `src/quality/business/check_silver_business_rules.py` | Vérification de règles métier |
| Diagnostic des montants POS | `src/quality/business/diagnose_pos_amounts.py` | Analyse des écarts de montants |
| Comparaison Bronze/Silver | `src/quality/business/compare_bronze_silver_pos.py` | Comparaison des données de ventes |
| Rapport Gold | `data/reports/gold/` | Rapports de construction des dimensions et des faits |
| Architecture | `docs/architecture/L1_Dossier_Architecture_NeoRetail.md` | Dossier d'architecture |
| Docker | `infra/docker/compose.yaml` | Configuration de services Docker |
| MLflow | `infra/mlflow/README.md` | Documentation du composant MLflow |

## 7. Qualité et traçabilité

Les rapports générés sont conservés dans `data/reports/`. Ils permettent de suivre les résultats des contrôles et de justifier les décisions de traitement.

Les anomalies identifiées doivent être classées comme corrigées, acceptées, à surveiller ou à investiguer. Toute correction doit s'appuyer sur une règle métier documentée.

Les rapports de contrôle sont des éléments de traçabilité : ils ne remplacent pas les données ni les scripts qui les produisent.

## 8. Environnement technique

Composants déjà présents dans le dépôt :
- Python pour les scripts de traitement et de contrôle ;
- fichiers Parquet pour les données préparées ;
- Docker Compose pour la configuration de services ;
- MLflow pour le suivi des expériences, avec une configuration Docker présente dans le projet.

L'état opérationnel d'Airflow, du streaming, de dbt, de la solution BI et de la supervision doit être confirmé avant de considérer ces composants comme intégrés.

## 9. Prochaines étapes

1. Vérifier et documenter les commandes d'exécution des scripts existants.
2. Confirmer la reproductibilité des constructions Silver et Gold.
3. Mettre en place l'orchestration des traitements avec Airflow.
4. Intégrer et tester les transformations analytiques avec dbt, si cette solution est retenue.
5. Définir les contrôles qualité automatisés et les contrats de données.
6. Étudier l'intégration du flux de données événementiel avec Kafka ou Redpanda.
7. Développer le pipeline de machine learning et son suivi avec MLflow.
8. Construire un tableau de bord décisionnel et mettre en place la supervision.
9. Préparer une démonstration de bout en bout.

Ces étapes sont des objectifs à réaliser ou à confirmer, et non des fonctionnalités déclarées comme terminées.

## 10. Règles de travail

- Conserver les données sources et éviter les modifications non justifiées.
- Ne pas supprimer les volumes Docker contenant des données persistantes.
- Contrôler les rapports après chaque traitement.
- Documenter les décisions métier et les anomalies connues.
- Distinguer les composants opérationnels des composants simplement configurés.
