# Exécution de la PHASE 3 — Schema Management

Ce document consigne l'ensemble des actions, configurations, créations de fichiers et validations réalisées pour la **Phase 3** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 3 a pour objectif d'instaurer un contrat de données (Data Contract) déclaratif, évolutif et strict entre la couche Bronze et la couche Silver :
* Définition des schémas canoniques Silver en fichiers **YAML** déclaratifs (`configs/schemas/orders.yaml`, `customers.yaml`, `products.yaml`).
* Conception d'un moteur de mapping capable de résoudre aussi bien les structures imbriquées d'événements CDC Debezium (`after.order_id`) que les colonnes directes de fichiers plats (`order_id`).
* Validation rigoureuse de la présence des colonnes obligatoires (`required: true`) avec levée d'exceptions explicites (`SchemaValidationError`) en cas de non-conformité.
* Casting typé strict vers les types PySpark/Iceberg (`DecimalType(12, 2)`, `TimestampType`, `LongType`).
* Enrichissement systématique par les métadonnées de traçabilité CDC (`_source_lsn`, `_last_op`, `is_deleted`, `_ingested_at`).

---

## 2. Commandes Exécutées & Chronologie

### A. Création des contrats YAML et du code source
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# 1. Contrats déclaratifs YAML
# - configs/schemas/orders.yaml
# - configs/schemas/customers.yaml
# - configs/schemas/products.yaml

# 2. Modules de gestion de schémas
# - src/schemas/models.py (modèles Pydantic)
# - src/schemas/type_converter.py (convertisseur string -> DataType PySpark)
# - src/schemas/manager.py (SchemaManager)
# - src/schemas/__init__.py
```

### B. Exécution des tests unitaires sur l'hôte
```bash
source .venv/bin/activate
pytest tests/unit/test_schemas.py
```
**Résultat des tests unitaires :**
```text
tests/unit/test_schemas.py::test_parse_spark_type_basics PASSED          [ 20%]
tests/unit/test_schemas.py::test_parse_spark_type_decimal PASSED         [ 40%]
tests/unit/test_schemas.py::test_parse_spark_type_unsupported PASSED     [ 60%]
tests/unit/test_schemas.py::test_table_schema_config_validation PASSED   [ 80%]
tests/unit/test_schemas.py::test_load_orders_yaml PASSED                 [100%]
============================== 5 passed in 0.05s ===============================
```

### C. Exécution du test E2E en conteneur Spark (`make test-schemas`)
```bash
docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_schema_management_e2e.py
```
**Résultat du test Spark en conteneur :**
```text
============================================================
DEMARRAGE DU TEST E2E DU SCHEMA MANAGEMENT (PHASE 3)
============================================================
[1/4] Schema 'orders' charge avec succes (Version 1.0.0).
      Colonnes obligatoires: ['order_id', 'customer_id', 'status', 'total_amount', 'created_at']

[2/4] Test de mapping sur evenements CDC Debezium (JSON)...
  -> Schema Silver canonique produit depuis CDC :
root
 |-- order_id: long (nullable = true)
 |-- customer_id: long (nullable = true)
 |-- status: string (nullable = true)
 |-- total_amount: decimal(12,2) (nullable = true)
 |-- created_at: timestamp (nullable = true)
 |-- updated_at: timestamp (nullable = true)
 |-- is_deleted: boolean (nullable = true)
 |-- _last_op: string (nullable = true)
 |-- _source_lsn: long (nullable = true)
 |-- _ingested_at: timestamp (nullable = false)

+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+
|order_id|customer_id|status |total_amount|created_at         |updated_at         |is_deleted|_last_op|_source_lsn|_ingested_at              |
+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+
|2001    |601        |PENDING|85.00       |2026-11-20 10:00:00|2026-11-20 10:00:00|false     |c       |1001       |2026-11-21 18:34:04.025361|
|2001    |601        |PAID   |85.00       |2026-11-20 10:00:00|2026-11-20 10:10:00|false     |u       |1002       |2026-11-21 18:34:04.025361|
|2002    |602        |PAID   |199.90      |2026-11-20 10:30:00|2026-11-20 10:30:00|false     |c       |1003       |2026-11-21 18:34:04.025361|
+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------------+

[3/4] Test de mapping sur source plate CSV (fallback transparent)...
  -> Schema Silver canonique produit depuis CSV :
+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------+
|order_id|customer_id| status|total_amount|         created_at|         updated_at|is_deleted|_last_op|_source_lsn|        _ingested_at|
+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------+
|    1001|        501|   PAID|      129.99|2026-11-20 10:00:00|2026-11-20 10:05:00|     false|       r|          0|2026-11-21 18:34:...|
|    1002|        502|PENDING|       49.50|2026-11-20 10:15:00|2026-11-20 10:15:00|     false|       r|          0|2026-11-21 18:34:...|
|    1003|        503|SHIPPED|      240.00|2026-11-20 11:00:00|2026-11-20 14:30:00|     false|       r|          0|2026-11-21 18:34:...|
+--------+-----------+-------+------------+-------------------+-------------------+----------+--------+-----------+--------------------+

[4/4] Test de detection de violation de schema...
  -> Rejet confirme : Contrat de schema viole pour 'orders'. Colonnes obligatoires manquantes: order_id (candidats cherches: ['after.order_id', 'before.order_id', 'order_id'])

============================================================
✅ TOUS LES TESTS DU SCHEMA MANAGEMENT ONT REUSSI !
Contrats YAML, mapping transparent CDC/Flat, casting strict et rejets validés.
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
============================== 18 passed in 0.38s ==============================
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `orders.yaml` | `configs/schemas/` | Contrat déclaratif de la table `orders` (grain commande, types, mappings Debezium/CSV) |
| `customers.yaml` | `configs/schemas/` | Contrat déclaratif de la table `customers` |
| `products.yaml` | `configs/schemas/` | Contrat déclaratif de la table `products` |
| `models.py` | `src/schemas/` | Modèles Pydantic de validation des fichiers YAML de configuration |
| `type_converter.py` | `src/schemas/` | Moteur de conversion des types textuels vers `pyspark.sql.types.DataType` |
| `manager.py` | `src/schemas/` | `SchemaManager` : validation, mapping, casting et enrichissement CDC |
| `test_schemas.py` | `tests/unit/` | Tests unitaires de parsing YAML et conversion de types |
| `test_schema_management_e2e.py` | `docker/spark/scripts/` | Test d'intégration PySpark démontrant le mapping CDC et le rejet d'anomalies |
| `PHASE_3_SCHEMA_MANAGEMENT.md` | `docs/` | Rapport technique détaillé de la phase de gestion des schémas |
| `Makefile` | `projet/Makefile` | Ajout de la cible `make test-schemas` |

---

## 4. Structure Finale Obtenue

```
projet/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile                  # Cibles: docker-up, test-adapters, test-schemas, lint, test
├── pyproject.toml
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── PHASE_0_EXECUTION.md
├── PHASE_1_EXECUTION.md
├── PHASE_2_EXECUTION.md
├── PHASE_3_EXECUTION.md      # Journal de la Phase 3 (ce fichier)
├── configs/
│   ├── quality_rules/
│   └── schemas/              # Contrats déclaratifs YAML
│       ├── customers.yaml
│       ├── orders.yaml
│       └── products.yaml
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
│           └── test_schema_management_e2e.py # Test E2E mapping & casting
├── docs/
│   ├── architecture/
│   ├── PHASE_1_INFRASTRUCTURE.md
│   ├── PHASE_2_SOURCE_ADAPTERS.md
│   └── PHASE_3_SCHEMA_MANAGEMENT.md
├── src/
│   ├── adapters/
│   ├── api/
│   ├── orchestrator/
│   ├── quality/
│   ├── schemas/              # Moteur de validation et mapping
│   │   ├── __init__.py
│   │   ├── manager.py
│   │   ├── models.py
│   │   └── type_converter.py
│   ├── storage/
│   ├── transformers/
│   └── utils/
└── tests/
    ├── conftest.py
    ├── integration/
    │   └── test_infrastructure.py
    └── unit/
        ├── test_adapters.py
        ├── test_baseline.py
        └── test_schemas.py   # Tests unitaires des schémas
```

---

## 5. Prochaine Étape : PHASE 4 — PySpark Transformation Engine

Le contrat de schéma étant garanti et standardisé, la prochaine étape consiste à construire le moteur de transformations métier :
1. **Null handling** et imputations contrôlées.
2. **Déduplication par commande** : application de la règle d'or CDC basée sur le dernier LSN (`source.lsn`).
3. **Gestion des suppressions logiques (soft deletes)** : traitement des événements `op == 'd'`.
4. **Standardisation métier** : statuts en minuscules/majuscules homogènes, validation des montants positifs.
5. Implémentation sur le dataset **Orders**, puis généralisation modulaire pour **Customers** et **Products**.
