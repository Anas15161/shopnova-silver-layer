# Exécution de la PHASE 4 — PySpark Transformation Engine

Ce document consigne l'ensemble des actions, configurations, créations de fichiers et validations réalisées pour la **Phase 4** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 4 a pour objectif d'implémenter le moteur de transformation métier, de standardisation et de déduplication des données de la couche Silver :
* **Déduplication Stricte par LSN** : application de la règle d'or CDC ordonnant les événements par `source.lsn` descendant via une fenêtre analytique Spark (`Window.partitionBy(primary_key).orderBy(F.col("_source_lsn").desc())`).
* **Null Handling** : exclusion rigoureuse des lignes dont les clés primaires ou étrangères indispensables sont manquantes.
* **Standardisation Métier** : normalisation des statuts de commande en minuscules (`pending`, `paid`, `shipped`, `delivered`, `cancelled`, `refunded`), nettoyage des espaces (`trim`), formatage des identités client en Title Case (`F.initcap`).
* **Validation Métier** : vérification que les montants et quantités sont positifs ou nuls.
* **Enrichissement de Données** : dérivation de la colonne de partitionnement Iceberg `created_date = F.to_date(created_at)`, indicateurs booléens (`is_paid`, `is_final_status`, `is_in_stock`).
* **Architecture Réutilisable** : classe abstraite `BaseTransformer`, fabrique `TransformerFactory` et implémentations dédiées pour `Orders`, `Customers` et `Products`.

---

## 2. Commandes Exécutées & Chronologie

### A. Création du package et des transformers
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# 1. Modules de transformation sous src/transformers/
# - src/transformers/base.py (BaseTransformer avec déduplication par LSN)
# - src/transformers/orders_transformer.py (OrdersTransformer)
# - src/transformers/customers_transformer.py (CustomersTransformer)
# - src/transformers/products_transformer.py (ProductsTransformer)
# - src/transformers/factory.py (TransformerFactory)
# - src/transformers/__init__.py
```

### B. Exécution des tests unitaires sur l'hôte
```bash
source .venv/bin/activate
pytest tests/unit/test_transformers.py
```
**Résultat des tests unitaires :**
```text
tests/unit/test_transformers.py::test_factory_returns_orders_transformer PASSED    [ 20%]
tests/unit/test_transformers.py::test_factory_returns_customers_transformer PASSED [ 40%]
tests/unit/test_transformers.py::test_factory_returns_products_transformer PASSED  [ 60%]
tests/unit/test_transformers.py::test_factory_case_insensitive PASSED             [ 80%]
tests/unit/test_transformers.py::test_factory_unsupported_dataset PASSED          [100%]
============================== 5 passed in 0.05s ===============================
```

### C. Exécution du test E2E en conteneur Spark (`make test-transformers`)
```bash
docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_transformers_e2e.py
```
**Résultat de l'exécution PySpark en conteneur :**
```text
============================================================
DEMARRAGE DU TEST E2E DU TRANSFORMATION ENGINE (PHASE 4)
============================================================
[1/3] Test du OrdersTransformer...
  -> Événements bruts CDC avant déduplication: 3
  -> Commandes uniques après déduplication par LSN: 2
+--------+-----------+------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+------------+-------+---------------+
|order_id|customer_id|status|total_amount|created_at         |updated_at         |is_deleted|_last_op|_source_lsn|_ingested_at              |created_date|is_paid|is_final_status|
+--------+-----------+------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+------------+-------+---------------+
|2001    |601        |paid  |85.00       |2026-11-20 10:00:00|2026-11-20 10:10:00|false     |u       |1002       |2026-11-21 19:39:38.212031|2026-11-20  |true   |false          |
|2002    |602        |paid  |199.90      |2026-11-20 10:30:00|2026-11-20 10:30:00|false     |c       |1003       |2026-11-21 19:39:38.212031|2026-11-20  |true   |false          |
+--------+-----------+------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+------------+-------+---------------+

  -> Commande 2001 : Statut retenu = 'paid' (LSN = 1002)

[2/3] Test du CustomersTransformer...
+-----------+----------+---------+----------------+----------+-----+-------------------+----------+--------+-----------+-------------------------+-----------+
|customer_id|first_name|last_name|email           |phone     |city |created_at         |is_deleted|_last_op|_source_lsn|_ingested_at             |full_name  |
+-----------+----------+---------+----------------+----------+-----+-------------------+----------+--------+-----------+-------------------------+-----------+
|501        |Anas      |Haddou   |anas@example.com|0600000000|Rabat|2026-11-20 10:00:00|false     |u       |20         |2026-11-21 19:39:40.25779|Anas Haddou|
+-----------+----------+---------+----------------+----------+-----+-------------------+----------+--------+-----------+-------------------------+-----------+

[3/3] Test du ProductsTransformer...
+----------+-----------------+-----------+------+--------------+-------------------+----------+--------+-----------+--------------------------+-----------+
|product_id|name             |category   |price |stock_quantity|created_at         |is_deleted|_last_op|_source_lsn|_ingested_at              |is_in_stock|
+----------+-----------------+-----------+------+--------------+-------------------+----------+--------+-----------+--------------------------+-----------+
|10        |Laptop Pro       |electronics|999.99|15            |2026-11-20 10:00:00|false     |c       |1          |2026-11-21 19:39:41.505831|true       |
|11        |Out of Stock Item|accessories|19.99 |0             |2026-11-20 10:00:00|false     |c       |2          |2026-11-21 19:39:41.505831|false      |
+----------+-----------------+-----------+------+--------------+-------------------+----------+--------+-----------+--------------------------+-----------+

============================================================
✅ TOUS LES TESTS DU TRANSFORMATION ENGINE ONT REUSSI !
Déduplication stricte par LSN, normalisations et enrichissements validés.
============================================================
```

### D. Validation globale Pytest & Linting sur l'hôte
```bash
black --check src tests
flake8 src tests --max-line-length=100 --extend-ignore=E203,W503
pytest
```
**Résultat :**
```text
============================== 23 passed in 0.37s ==============================
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `base.py` | `src/transformers/` | Classe abstraite `BaseTransformer` définissant `deduplicate_by_lsn()`, `trim_all_strings()`, `fill_null_defaults()` |
| `orders_transformer.py` | `src/transformers/` | Moteur métier Orders (déduplication LSN, statuts minuscules, calcul `created_date`, flags `is_paid`) |
| `customers_transformer.py` | `src/transformers/` | Moteur métier Customers (déduplication LSN, Title Case pour nom/prénom, concaténation `full_name`) |
| `products_transformer.py` | `src/transformers/` | Moteur métier Products (déduplication LSN, validation `price >= 0`, calcul `is_in_stock`) |
| `factory.py` | `src/transformers/` | Fabrique dynamique `TransformerFactory` |
| `test_transformers.py` | `tests/unit/` | Tests unitaires de la fabrique et des classes de transformation |
| `test_transformers_e2e.py` | `docker/spark/scripts/` | Test d'intégration PySpark démontrant la déduplication réelle par LSN et enrichissement |
| `PHASE_4_TRANSFORMATION_ENGINE.md` | `docs/` | Rapport technique détaillé du moteur de transformation |
| `Makefile` | `projet/Makefile` | Ajout de la cible `make test-transformers` |

---

## 4. Structure Finale Obtenue

```
projet/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile                  # Cibles: docker-up, test-adapters, test-schemas, test-transformers, lint, test
├── pyproject.toml
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── BRONZE_ADAPTABILITY_GUIDE.md # Guide d'adaptabilité et de découplage
├── PHASE_0_EXECUTION.md
├── PHASE_1_EXECUTION.md
├── PHASE_2_EXECUTION.md
├── PHASE_3_EXECUTION.md
├── PHASE_4_EXECUTION.md      # Journal de la Phase 4 (ce fichier)
├── configs/
│   ├── quality_rules/
│   └── schemas/
├── data/
│   ├── bronze_sample/
│   ├── quarantine/
│   └── warehouse/
├── docker/
│   ├── minio/
│   └── spark/
│       ├── conf/
│       └── scripts/
│           ├── test_infrastructure.py
│           ├── test_adapters_e2e.py
│           ├── test_schema_management_e2e.py
│           └── test_transformers_e2e.py # Test E2E déduplication LSN & transformations
├── docs/
│   ├── architecture/
│   │   └── BRONZE_ADAPTABILITY_GUIDE.md
│   ├── PHASE_1_INFRASTRUCTURE.md
│   ├── PHASE_2_SOURCE_ADAPTERS.md
│   ├── PHASE_3_SCHEMA_MANAGEMENT.md
│   └── PHASE_4_TRANSFORMATION_ENGINE.md
├── src/
│   ├── adapters/
│   ├── api/
│   ├── orchestrator/
│   ├── quality/
│   ├── schemas/
│   ├── storage/
│   ├── transformers/         # Moteur de transformation métier
│   │   ├── __init__.py
│   │   ├── base.py           # Déduplication LSN générique
│   │   ├── customers_transformer.py
│   │   ├── factory.py
│   │   ├── orders_transformer.py
│   │   └── products_transformer.py
│   └── utils/
└── tests/
    ├── conftest.py
    ├── integration/
    └── unit/
        ├── test_adapters.py
        ├── test_baseline.py
        ├── test_schemas.py
        └── test_transformers.py # Tests unitaires transformers
```

---

## 5. Prochaine Étape : PHASE 5 — Data Quality Framework

Les données étant transformées, nettoyées et dédupliquées, la prochaine étape consiste à ériger le pare-feu de qualité de la Silver Layer :
1. Implémenter les 5 dimensions de la qualité de données :
   * **Completeness** (complétude des champs obligatoires).
   * **Uniqueness** (unicité stricte de la clé primaire après transformation).
   * **Validity** (règles de validité métier, format email, montants).
   * **Consistency** (cohérence des dates : `created_at <= updated_at`).
   * **Accuracy** (précision des montants).
2. Concevoir le stockage et l'isolation automatique des enregistrements rejetés vers la **Quarantaine MinIO** (`s3a://quarantine/`).
3. Générer les rapports de métriques d'audit qualité (taux de rejets, causes de non-conformité).
