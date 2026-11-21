# Exécution de la PHASE 2 — Source Adapters

Ce document consigne l'ensemble des actions, configurations, créations de fichiers et validations réalisées pour la **Phase 2** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 2 a pour objectif de développer un système d'ingestion modulaire, découplé et indépendant du format de stockage de la couche Bronze :
* Permettre à la Silver Layer de lire indifféremment des flux ou fichiers **CSV**, **JSON** (notamment les événements CDC Debezium), **Parquet** et des tables **Apache Iceberg**.
* Définir un contrat d'interface rigoureux via la classe abstraite `BaseSourceAdapter`.
* Implémenter une fabrique dynamique `SourceAdapterFactory` dotée de la détection automatique de format selon l'URI ou le nom de table.
* Garantir l'indépendance de notre périmètre Silver vis-à-vis des livrables finaux du responsable de la Bronze Layer.
* Valider la lecture des 4 formats au moyen de tests unitaires sur l'hôte et de tests d'intégration PySpark en conteneur.

---

## 2. Commandes Exécutées & Chronologie

### A. Implémentation des adaptateurs et de la factory
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# Création des modules d'ingestion sous src/adapters/
# - src/adapters/base.py (classe abstraite)
# - src/adapters/csv_adapter.py
# - src/adapters/json_adapter.py (Debezium CDC)
# - src/adapters/parquet_adapter.py
# - src/adapters/iceberg_adapter.py (Time Travel)
# - src/adapters/factory.py (SourceAdapterFactory)
# - src/utils/spark.py (SparkSession builder)
```

### B. Ajout du montage de données et relance Docker
```bash
# Mise à jour du docker-compose.yml pour monter ./data:/home/iceberg/data
DOCKER_API_VERSION=1.45 docker compose up -d
```

### C. Exécution du test E2E en conteneur Spark (`make test-adapters`)
```bash
docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_adapters_e2e.py
```
**Résultat de l'exécution PySpark :**
```text
============================================================
DEMARRAGE DU TEST E2E DES SOURCE ADAPTERS (PHASE 2)
============================================================
[1/5] Test du CSVSourceAdapter...
  -> Lignes lues depuis CSV: 3
+--------+-----------+-------+------------+-------------------+-------------------+
|order_id|customer_id| status|total_amount|         created_at|         updated_at|
+--------+-----------+-------+------------+-------------------+-------------------+
|    1001|        501|   PAID|      129.99|2026-11-20 10:00:00|2026-11-20 10:05:00|
|    1002|        502|PENDING|        49.5|2026-11-20 10:15:00|2026-11-20 10:15:00|
|    1003|        503|SHIPPED|       240.0|2026-11-20 11:00:00|2026-11-20 14:30:00|
+--------+-----------+-------+------------+-------------------+-------------------+

[2/5] Test du JSONSourceAdapter (evenements CDC)...
  -> Lignes lues depuis JSON CDC: 3
+----------------------------------------------------------------------+----------------------------------------------------------------------+---+--------------------------------------------------------+-------------+
|after                                                                 |before                                                                |op |source                                                  |ts_ms        |
+----------------------------------------------------------------------+----------------------------------------------------------------------+---+--------------------------------------------------------+-------------+
|{2026-11-20T10:00:00Z, 601, 2001, PENDING, 85.0, 2026-11-20T10:00:00Z}|NULL                                                                  |c  |{postgresql, 1001, shopnova, 1732100000000, 2.4.0.Final}|1732100000000|
|{2026-11-20T10:00:00Z, 601, 2001, PAID, 85.0, 2026-11-20T10:10:00Z}   |{2026-11-20T10:00:00Z, 601, 2001, PENDING, 85.0, 2026-11-20T10:00:00Z}|u  |{postgresql, 1002, shopnova, 1732100600000, 2.4.0.Final}|1732100600000|
|{2026-11-20T10:30:00Z, 602, 2002, PAID, 199.9, 2026-11-20T10:30:00Z}  |NULL                                                                  |c  |{postgresql, 1003, shopnova, 1732101800000, 2.4.0.Final}|1732101800000|
+----------------------------------------------------------------------+----------------------------------------------------------------------+---+--------------------------------------------------------+-------------+

[3/5] Test du ParquetSourceAdapter...
  -> Lignes lues depuis Parquet: 3

[4/5] Test du IcebergSourceAdapter...
  -> Lignes lues depuis table Iceberg: 2
+---+------+--------------------+
| id|  name|          created_at|
+---+------+--------------------+
|101|Item A|2026-11-21 18:04:...|
|102|Item B|2026-11-21 18:04:...|
+---+------+--------------------+

[5/5] Test de l'auto-detection de format via Factory...
============================================================
✅ TOUS LES TESTS DES SOURCE ADAPTERS ONT REUSSI !
CSV, JSON (CDC), Parquet et Iceberg sont geres de maniere agnostique.
============================================================
```

### D. Validation de la qualité de code & Pytest sur l'hôte
```bash
source .venv/bin/activate
black --check src tests
flake8 src tests --max-line-length=100 --extend-ignore=E203,W503
pytest
```
**Résultat des tests :**
```text
tests/integration/test_infrastructure.py::test_minio_health PASSED             [  7%]
tests/integration/test_infrastructure.py::test_iceberg_rest_catalog_config PASSED [ 15%]
tests/integration/test_infrastructure.py::test_minio_buckets_exist PASSED       [ 23%]
tests/unit/test_adapters.py::test_factory_returns_csv_adapter PASSED           [ 30%]
tests/unit/test_adapters.py::test_factory_returns_json_adapter PASSED          [ 38%]
tests/unit/test_adapters.py::test_factory_returns_parquet_adapter PASSED       [ 46%]
tests/unit/test_adapters.py::test_factory_returns_iceberg_adapter PASSED       [ 53%]
tests/unit/test_adapters.py::test_factory_case_insensitive PASSED              [ 61%]
tests/unit/test_adapters.py::test_factory_unsupported_format PASSED            [ 69%]
tests/unit/test_adapters.py::test_detect_format_from_path PASSED               [ 76%]
tests/unit/test_baseline.py::test_project_structure PASSED                     [ 84%]
tests/unit/test_baseline.py::test_src_modules_exist PASSED                     [ 92%]
tests/unit/test_baseline.py::test_env_example_exists PASSED                    [100%]
============================== 13 passed in 0.28s ==============================
```

### E. Enregistrement Git & Commit conventionnel
```bash
git add Makefile docker-compose.yml requirements.txt src/ docker/spark/scripts/test_adapters_e2e.py docs/PHASE_2_SOURCE_ADAPTERS.md tests/unit/test_adapters.py
git commit -m "feat(silver): implement modular source adapters for CSV, JSON, Parquet, and Iceberg"
```
**Résultat Git :**
```text
[main 5175843] feat(silver): implement modular source adapters for CSV, JSON, Parquet, and Iceberg
 14 files changed, 509 insertions(+), 10 deletions(-)
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `base.py` | `src/adapters/` | Définition de l'interface abstraite `BaseSourceAdapter` et de la méthode `read()` |
| `csv_adapter.py` | `src/adapters/` | Lecteur de fichiers CSV avec en-têtes et inférence de types |
| `json_adapter.py` | `src/adapters/` | Lecteur de JSON et flux CDC Debezium (`op`, `before`, `after`, `source.lsn`) |
| `parquet_adapter.py` | `src/adapters/` | Lecteur de fichiers Parquet optimisé en colonnes |
| `iceberg_adapter.py` | `src/adapters/` | Lecteur de tables Iceberg avec support des requêtes Time Travel |
| `factory.py` | `src/adapters/` | Fabrique d'instanciation dynamique et détection automatique de format |
| `spark.py` | `src/utils/` | Utilitaire de récupération/initialisation de la SparkSession |
| `orders_sample.csv` | `data/bronze_sample/` | Jeu de test CSV pour validation locale (non versionné) |
| `orders_cdc.json` | `data/bronze_sample/` | Échantillon d'événements CDC Debezium PostgreSQL (non versionné) |
| `test_adapters.py` | `tests/unit/` | Tests unitaires de la fabrique, instanciation et gestion des exceptions |
| `test_adapters_e2e.py` | `docker/spark/scripts/` | Script d'intégration exécuté sous Spark testant la lecture réelle des 4 formats |
| `PHASE_2_SOURCE_ADAPTERS.md` | `docs/` | Rapport technique complet de la phase d'ingestion modulaire |

---

## 4. Structure Finale Obtenue

```
projet/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile                  # Cibles: docker-up, docker-test, test-adapters, lint, test
├── pyproject.toml
├── README.md
├── requirements.txt          # PySpark, FastAPI, PyIceberg, PyArrow, Pydantic, Boto3
├── requirements-dev.txt      # Pytest, Black, Flake8, isort
├── PHASE_0_EXECUTION.md      # Journal de la Phase 0
├── PHASE_1_EXECUTION.md      # Journal de la Phase 1
├── PHASE_2_EXECUTION.md      # Journal de la Phase 2 (ce fichier)
├── configs/
│   ├── quality_rules/
│   └── schemas/
├── data/
│   ├── bronze_sample/        # Échantillons CSV et JSON CDC
│   ├── quarantine/
│   └── warehouse/
├── docker/
│   ├── minio/
│   └── spark/
│       ├── conf/
│       │   └── spark-defaults.conf
│       └── scripts/
│           ├── test_infrastructure.py
│           └── test_adapters_e2e.py   # Test E2E des adaptateurs sources
├── docs/
│   ├── architecture/
│   ├── PHASE_1_INFRASTRUCTURE.md
│   └── PHASE_2_SOURCE_ADAPTERS.md
├── src/
│   ├── adapters/             # Système d'ingestion agnostique
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── csv_adapter.py
│   │   ├── factory.py
│   │   ├── iceberg_adapter.py
│   │   ├── json_adapter.py
│   │   └── parquet_adapter.py
│   ├── api/
│   ├── orchestrator/
│   ├── quality/
│   ├── schemas/
│   ├── storage/
│   ├── transformers/
│   └── utils/
│       ├── __init__.py
│       └── spark.py          # Helper SparkSession
└── tests/
    ├── conftest.py
    ├── integration/
    │   └── test_infrastructure.py
    └── unit/
        ├── test_adapters.py  # Tests unitaires des adaptateurs
        └── test_baseline.py
```

---

## 5. Prochaine Étape : PHASE 3 — Schema Management

L'ingestion agnostique étant opérationnelle, la prochaine étape consiste à cadrer les données entrantes :
1. Définir les schémas canoniques Silver en fichiers déclaratifs **YAML** (`configs/schemas/orders.yaml`, `customers.yaml`, `products.yaml`).
2. Développer le moteur de validation et de casting strict des types de données.
3. Implémenter le mapping configurable des colonnes Bronze vers Silver.
4. Gérer l'évolution de schéma et la conformité des contrats de données.
