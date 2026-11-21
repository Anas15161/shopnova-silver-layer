# PHASE 3 — Schema Management : Contrats Déclaratifs et Mapping Bronze ➔ Silver

Ce document consigne la conception, les choix d'architecture, l'implémentation et les validations de la **Phase 3** du projet **ShopNova Silver Layer**.

---

## 1. Objectif

Établir un contrat de données (Data Contract) déclaratif, évolutif et strict entre la couche Bronze et la couche Silver :
* Externaliser les définitions de schémas canoniques en fichiers **YAML** (`configs/schemas/orders.yaml`, `customers.yaml`, `products.yaml`).
* Gérer les correspondances et chemins de fallback des colonnes Bronze vers Silver (ex: `after.order_id` pour Debezium CDC ou `order_id` pour un fichier plat).
* Effectuer le casting strict vers les types Spark/Iceberg (`DecimalType(12,2)`, `TimestampType`, `LongType`).
* Vérifier la présence obligatoire des colonnes métier et rejeter les jeux de données non conformes (`SchemaValidationError`).
* Enrichir automatiquement les enregistrements avec les métadonnées techniques CDC (`_source_lsn`, `_last_op`, `is_deleted`, `_ingested_at`).

---

## 2. Architecture Technique

```
┌────────────────────────────────────────────────────────────────────────┐
│                        PHASE 3 : SCHEMA MANAGEMENT                     │
│                                                                        │
│   Données Bronze Brutes                    Schéma Canonique Silver     │
│   (CSV, JSON CDC Debezium)                 (Contrat d'Interface)       │
│   ┌───────────────────────┐                ┌───────────────────────┐   │
│   │ after.order_id        │                │ order_id (BIGINT)     │   │
│   │ after.total_amount    │───[ Mapping ]─▶│ total_amount (DECIMAL)│   │
│   │ source.lsn            │   [ & Casting] │ _source_lsn (BIGINT)  │   │
│   │ op                    │                │ _last_op (STRING)     │   │
│   └───────────────────────┘                │ _ingested_at (TS)     │   │
│                                            └───────────────────────┘   │
│                                                        ▲               │
│                                                        │               │
│                                          configs/schemas/orders.yaml   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Composants Implémentés

### A. Modèles Déclaratifs Pydantic (`src/schemas/models.py`)
* `ColumnDefinition` : nom cible, type canonique, booléen `required`, liste ordonnée `source_fields`.
* `TableSchemaConfig` : métadonnées de table, clé primaire, colonne de partitionnement, liste de colonnes et bloc `cdc_metadata`.

### B. Convertisseur de Types (`src/schemas/type_converter.py`)
* Traduit les types déclaratifs YAML (`bigint`, `string`, `decimal(p, s)`, `timestamp`, `boolean`) en instances concrètes de `pyspark.sql.types.DataType`.
* Analyse regex pour les types paramétrés (`decimal(12,2)` ➔ `DecimalType(12, 2)`).

### C. Moteur de Mapping & Validation (`src/schemas/manager.py`)
* `SchemaManager` :
  1. Charge le fichier YAML associé à la table.
  2. Résout le chemin de la colonne (champs imbriqués Debezium comme `after.order_id` ou colonnes plates directes).
  3. Lève `SchemaValidationError` si au moins une colonne obligatoire est manquante.
  4. Applique le casting typé (gestion robuste des timestamps ISO 8601).
  5. Injecte les colonnes de traçabilité CDC : `_source_lsn`, `_last_op`, `is_deleted` et `_ingested_at`.

---

## 4. Tests et Validations Réalisés

### A. Tests Unitaires Hôte (Pytest)
```bash
pytest tests/unit/test_schemas.py
```
```text
tests/unit/test_schemas.py::test_parse_spark_type_basics PASSED          [ 20%]
tests/unit/test_schemas.py::test_parse_spark_type_decimal PASSED         [ 40%]
tests/unit/test_schemas.py::test_parse_spark_type_unsupported PASSED     [ 60%]
tests/unit/test_schemas.py::test_table_schema_config_validation PASSED   [ 80%]
tests/unit/test_schemas.py::test_load_orders_yaml PASSED                 [100%]
============================== 5 passed in 0.05s ===============================
```

### B. Test E2E PySpark en Conteneur (`make test-schemas`)
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
