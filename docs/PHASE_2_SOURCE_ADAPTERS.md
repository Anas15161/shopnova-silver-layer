# PHASE 2 — Source Adapters : Ingestion Modulaire Multi-Format

Ce document consigne la conception, les choix d'architecture, l'implémentation et les validations de la **Phase 2** du projet **ShopNova Silver Layer**.

---

## 1. Objectif

Construire un système d'ingestion de données modulaire, découplé et totalement agnostique du format de stockage sous-jacent :
* Capacité à lire indifféremment des sources aux formats **CSV**, **JSON** (notamment les flux d'événements CDC Debezium), **Parquet** et des tables **Apache Iceberg**.
* Définition d'un contrat commun via une classe abstraite `BaseSourceAdapter`.
* Implémentation d'un registre et d'une fabrique `SourceAdapterFactory` avec détection automatique du format basée sur le chemin ou l'URI.
* Rendre la Silver Layer immédiatement testable et indépendante des choix finaux du responsable de la couche Bronze.

---

## 2. Architecture Technique

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SHOPNOVA SOURCE INGESTION                       │
│                                                                        │
│                ┌──────────────────────────────────────┐                │
│                │        SourceAdapterFactory          │                │
│                │   .get_adapter(format, spark)        │                │
│                │   .detect_format_from_path(uri)      │                │
│                └──────────────────┬───────────────────┘                │
│                                   │ Polymorphisme                      │
│                                   ▼                                    │
│                ┌──────────────────────────────────────┐                │
│                │          BaseSourceAdapter           │                │
│                │  .read(path, options) -> DataFrame   │                │
│                └──────────────────┬───────────────────┘                │
│                                   │                                    │
│        ┌──────────────────┬───────┴──────────┬──────────────────┐      │
│        ▼                  ▼                  ▼                  ▼      │
│ ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌───────────┐ │
│ │  CSVAdapter  │   │ JSONAdapter  │   │ParquetAdapter│   │  Iceberg  │ │
│ │ (CSV / TSV)  │   │(Debezium CDC)│   │  (Columnar)  │   │ (Catalog) │ │
│ └──────────────┘   └──────────────┘   └──────────────┘   └───────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Composants et Décisions d'Implémentation

### A. Interface `BaseSourceAdapter` (`src/adapters/base.py`)
Définit la méthode `read(path: str, options: Optional[Dict[str, Any]]) -> DataFrame` et la propriété `format_name -> str`.

### B. Adaptateur CSV (`CSVSourceAdapter`)
* Gère l'en-tête (`header=true`), l'inférence de schéma et le mode `PERMISSIVE` par défaut.
* Options personnalisables : séparateur (`delimiter`), encodage, quotes.

### C. Adaptateur JSON CDC (`JSONSourceAdapter`)
* Conçu pour ingérer le schéma d'enveloppe CDC Debezium (contenant `op`, `before`, `after`, `source.lsn`, `ts_ms`).
* Supporte le format JSON-lines multi-enregistrements.

### D. Adaptateur Parquet (`ParquetSourceAdapter`)
* Lecture optimisée en colonnes avec préservation native du typage et statistiques de fichiers.

### E. Adaptateur Apache Iceberg (`IcebergSourceAdapter`)
* Lit soit une table via le catalogue REST (`lake.silver.orders`), soit un chemin direct de métadonnées.
* Supporte les options de **Time Travel** : `as_of_timestamp` et `snapshot_id`.

### F. Fabrique `SourceAdapterFactory` (`src/adapters/factory.py`)
* Instanciation dynamique sans hardcoding (`get_adapter("csv", spark)`).
* Auto-détection du format basée sur l'extension ou le nommage (`.csv`, `.json`, `.parquet`, identifiant `namespace.table`).

---

## 4. Tests et Validations Réalisés

### A. Tests Unitaires Hôte (Pytest)
Couverture complète de la fabrique, des instanciations et de l'auto-détection :
```bash
pytest tests/unit/test_adapters.py
```
```text
tests/unit/test_adapters.py::test_factory_returns_csv_adapter PASSED     [ 14%]
tests/unit/test_adapters.py::test_factory_returns_json_adapter PASSED    [ 28%]
tests/unit/test_adapters.py::test_factory_returns_parquet_adapter PASSED [ 42%]
tests/unit/test_adapters.py::test_factory_returns_iceberg_adapter PASSED [ 57%]
tests/unit/test_adapters.py::test_factory_case_insensitive PASSED        [ 71%]
tests/unit/test_adapters.py::test_factory_unsupported_format PASSED      [ 85%]
tests/unit/test_adapters.py::test_detect_format_from_path PASSED         [100%]
```

### B. Test E2E PySpark en Conteneur (`make test-adapters`)
Exécution réelle sur des jeux de données d'échantillons (CSV, JSON Debezium CDC, Parquet et Table Iceberg créée à la volée) :
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
