# PHASE 4 — PySpark Transformation Engine

Ce document consigne la conception, l'implémentation et les validations de la **Phase 4** du projet **ShopNova Silver Layer**.

---

## 1. Objectif

Construire un moteur modulaire de transformations métier PySpark appliquant les règles d'or du traitement de données CDC et de standardisation Silver :
* **Null Handling** : élimination ou imputation rigoureuse des clés primaires et champs obligatoires nuls.
* **Déduplication Stricte par LSN (Règle d'or CDC)** : ordonnancement des événements par transaction PostgreSQL réelle (`source.lsn`) via une fenêtre analytique Spark (`Window.partitionBy(pk).orderBy(lsn.desc())`).
* **Standardisation Métier** : normalisation des statuts en minuscules (`pending`, `paid`, `shipped`, `cancelled`, `refunded`), nettoyage des chaînes (`trim`), formatage des noms (`initcap`).
* **Validation Métier** : vérification des montants (`total_amount >= 0`), stocks et prix.
* **Enrichissement de Données** : calcul de la date de partitionnement Iceberg (`created_date`), flags d'état (`is_paid`, `is_final_status`, `is_in_stock`).
* **Modularité multi-datasets** : implémentation dédiée pour **Orders**, **Customers** et **Products**.

---

## 2. Architecture Technique

```
┌────────────────────────────────────────────────────────────────────────┐
│                   PHASE 4 : PYSPARK TRANSFORMATION ENGINE              │
│                                                                        │
│                ┌──────────────────────────────────────┐                │
│                │          TransformerFactory          │                │
│                │     .get_transformer(dataset_name)   │                │
│                └──────────────────┬───────────────────┘                │
│                                   │ Polymorphisme                      │
│                                   ▼                                    │
│                ┌──────────────────────────────────────┐                │
│                │           BaseTransformer            │                │
│                │  • deduplicate_by_lsn(primary_key)   │                │
│                │  • trim_all_strings()                │                │
│                │  • fill_null_defaults()              │                │
│                └──────────────────┬───────────────────┘                │
│                                   │                                    │
│        ┌──────────────────────────┼──────────────────────────┐         │
│        ▼                          ▼                          ▼         │
│ ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────┐ │
│ │   OrdersTransformer  │  │ CustomersTransformer │  │ProductsTransf. │ │
│ │ • Dédup par LSN      │  │ • Dédup par LSN      │  │• Dédup par LSN │ │
│ │ • created_date       │  │ • initcap(nom/prénom)│  │• category trim │ │
│ │ • is_paid flag       │  │ • full_name concat   │  │• is_in_stock   │ │
│ │ • status lower       │  │ • email en minuscules│  │• price >= 0    │ │
│ └──────────────────────┘  └──────────────────────┘  └────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Composants Implémentés

### A. Classe Abstraite `BaseTransformer` (`src/transformers/base.py`)
Centralise les algorithmes réutilisables à travers tous les datasets :
* `deduplicate_by_lsn(primary_key, lsn_col="_source_lsn")` : garantit que pour chaque entité, seul le dernier état transactionnel est conservé.
* `trim_all_strings()` : supprime les espaces parasites sur toutes les colonnes de type `StringType`.
* `fill_null_defaults()` : impute des valeurs par défaut métier sécurisées.

### B. `OrdersTransformer` (`src/transformers/orders_transformer.py`)
* Filtre les identifiants nuls.
* Déduplique les événements de commande en conservant le dernier `_source_lsn`.
* Met les statuts en minuscules (`paid`, `pending`...).
* Ajoute la colonne `created_date = F.to_date(F.col("created_at"))` pour le partitionnement Iceberg.
* Génère les colonnes analytiques dérivées : `is_paid` et `is_final_status`.

### C. `CustomersTransformer` (`src/transformers/customers_transformer.py`)
* Déduplique par client (`customer_id`).
* Standardise les noms en Title Case (`F.initcap`), l'email en minuscules.
* Concatène le nom complet `full_name`.

### D. `ProductsTransformer` (`src/transformers/products_transformer.py`)
* Déduplique par produit (`product_id`).
* Valide que `price >= 0` et `stock_quantity >= 0`.
* Calcule le flag `is_in_stock = stock_quantity > 0`.

---

## 4. Tests et Validations Réalisés

### A. Tests Unitaires Hôte (Pytest - 23/23 tests)
```bash
pytest
```
```text
tests/unit/test_transformers.py::test_factory_returns_orders_transformer PASSED    [ 82%]
tests/unit/test_transformers.py::test_factory_returns_customers_transformer PASSED [ 86%]
tests/unit/test_transformers.py::test_factory_returns_products_transformer PASSED  [ 91%]
tests/unit/test_transformers.py::test_factory_case_insensitive PASSED             [ 95%]
tests/unit/test_transformers.py::test_factory_unsupported_dataset PASSED          [100%]
============================== 23 passed in 0.37s ==============================
```

### B. Test E2E PySpark en Conteneur (`make test-transformers`)
Démonstration de la déduplication par LSN sur `orders_cdc.json` (où la commande 2001 passe de `pending` à `paid`) :
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
