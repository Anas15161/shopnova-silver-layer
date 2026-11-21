# Exécution de la PHASE 6 — Apache Iceberg Storage

Ce document consigne l'ensemble des actions, configurations, implémentations et validations réalisées pour la **Phase 6** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 6 a pour objectif de concevoir, implémenter et valider la couche de stockage et de gestion transactionnelle **Apache Iceberg** pour les tables Silver de ShopNova :
* **Catalogue REST (`lake`) & MinIO S3** : persistance déclarative découplée dans le namespace `silver` (`lake.silver.orders`, `lake.silver.customers`, `lake.silver.products`).
* **Format-Version 2 (Merge-on-Read - MOR)** : gestion ACID des mutations au niveau ligne (`write.merge.mode = 'merge-on-read'`).
* **Stratégie de Partitionnement** :
  * `orders` : partitionné par `created_date` (gain de pruning sur les requêtes analytiques et jointures Gold).
  * `products` : partitionné par `category`.
  * `customers` : table non partitionnée.
* **Moteur d'Écriture ACID MERGE** : implémentation de `IcebergWriter` avec déduplication, fusion dynamique et **garde LSN anti-régression** (`source._source_lsn >= target._source_lsn`).
* **Time Travel & Audit Historique** : requêtes temporelles `VERSION AS OF` et `TIMESTAMP AS OF` validant la traçabilité des mutations de statuts (`paid` -> `refunded` -> audit historique).
* **Maintenance & Compaction** : exécution de la procédure `CALL lake.system.rewrite_data_files` pour consolider les petits fichiers Parquet.

---

## 2. Commandes Exécutées & Chronologie

### A. Implémentation du module `src/storage/`
* `src/storage/table_manager.py` : classe `IcebergTableManager` gérant la création des tables Iceberg, le typage, les propriétés MOR et les appels de maintenance système.
* `src/storage/iceberg_writer.py` : classe `IcebergWriter` implémentant le MERGE CDC avec clause de garde LSN, suppression automatique sur `is_deleted = true`, et écriture ACID.
* `src/storage/time_travel.py` : classe `IcebergTimeTravel` combinant le catalogue PyIceberg (inspection des snapshots et historique) et Spark SQL (lectures temporelles `snapshot-id` / `as-of-timestamp`).
* `src/storage/__init__.py` : exportations des classes `IcebergTableManager`, `IcebergWriter`, `IcebergTimeTravel`.

### B. Validation Unitaire sur l'Hôte
```bash
source .venv/bin/activate
pytest tests/unit/test_storage.py
```
**Résultat :**
```text
tests/unit/test_storage.py::test_iceberg_table_manager_namespace PASSED  [ 25%]
tests/unit/test_storage.py::test_iceberg_writer_merge_sql_generation PASSED [ 50%]
tests/unit/test_storage.py::test_iceberg_writer_composite_key PASSED     [ 75%]
tests/unit/test_storage.py::test_time_travel_timestamp_parsing PASSED    [100%]
============================== 4 passed in 0.38s ===============================
```

### C. Validation End-to-End dans le Conteneur Spark (`make test-storage`)
```bash
make test-storage
# Déclenche: docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_iceberg_storage_e2e.py
```
**Sortie d'exécution complète :**
```text
============================================================
DEMARRAGE DU TEST E2E APACHE ICEBERG STORAGE (PHASE 6)
============================================================

[1/6] Creation et verification des tables Silver (orders, customers, products)...
  -> Tables Silver creees avec format-version 2 (Merge-on-Read).
  -> Partitionnement de silver.orders par 'created_date' valide.

[2/6] Ingestion Lot 1 (Insert CDC) : Commande 4001 payee...
  -> Lot 1 insere avec succes. Snapshot ID = 8290261529435408446

[3/6] Ingestion Lot 2 (Update CDC) : Commande 4001 mutee en 'refunded'...
  -> Statut actuel de la commande 4001 : refunded (Snapshot ID: 1988095920825476669)

[4/6] Verification du Time Travel (TIMESTAMP AS OF / VERSION AS OF)...
  -> Statut historique au Snapshot 1 (8290261529435408446) : paid
  -> Requête SQL 'VERSION AS OF 8290261529435408446' validee avec succes !

[5/6] Tests de robustesse CDC : Idempotence et anti-regression LSN...
  -> Idempotence confirmee : aucun doublon cree apres rejeu.
  -> Garde LSN validee : le statut reste 'refunded' face a un evenement perime.
  -> Suppression CDC prise en compte avec succes (DELETE effectif).

[6/6] Execution de la maintenance Iceberg (rewrite_data_files)...
  -> Resultat compaction : [{'rewritten_data_files_count': 3, 'added_data_files_count': 0, 'rewritten_bytes_count': 11434, 'failed_data_files_count': 0}]

============================================================
✅ TOUS LES TESTS APACHE ICEBERG STORAGE (PHASE 6) ONT REUSSI !
Catalogue REST, Tables MOR, Time Travel, Idempotence et Maintenance valides.
============================================================
```

### D. Contrôle Qualité, Formatage et Couverture
```bash
source .venv/bin/activate
black --check src tests
flake8 src tests --max-line-length=100 --extend-ignore=E203,W503
pytest
```
**Résultat :** 30 tests unitaires et d'intégration validés sans aucun avertissement.

---

## 3. Fichiers Créés et Modifiés

| Fichier | Type d'action | Description |
| :--- | :--- | :--- |
| `src/storage/table_manager.py` | Création | Gestionnaire DDL, création tables Silver, partitionnement et compaction |
| `src/storage/iceberg_writer.py` | Création | Moteur d'écriture ACID MERGE, garde LSN et suppressions CDC |
| `src/storage/time_travel.py` | Création | Gestionnaire de snapshots PyIceberg et requêtes temporelles |
| `src/storage/__init__.py` | Modification | Exportation des classes publiques du module storage |
| `tests/unit/test_storage.py` | Création | Tests unitaires pour la génération SQL et parsing de dates |
| `docker/spark/scripts/test_iceberg_storage_e2e.py` | Création | Script de validation E2E (MERGE, Time Travel, Idempotence, Compaction) |
| `Makefile` | Modification | Ajout de la cible `make test-storage` |
| `docs/PHASE_6_ICEBERG_STORAGE.md` | Création | Documentation technique détaillée de la Phase 6 |
| `PHASE_6_EXECUTION.md` | Création | Journal complet de la Phase 6 |

---

## 4. Bilan & Statut

La **Phase 6 — Apache Iceberg Storage** est 100% achevée et validée.
* **Tables Opérationnelles** : `lake.silver.orders`, `customers` et `products` sont créées en format-version 2 (MOR).
* **Conformité CDC Stricte** : l'anti-régression LSN et l'idempotence totale sont garanties.
* **Traçabilité Temporelle Prouvée** : le Time Travel permet de naviguer dans l'historique complet des snapshots.
* **Prochaine étape** : **PHASE 7 — Pipeline Orchestration** (Orchestrateur Python unifié : Reader -> Schema -> Transformer -> Quality -> Quarantine -> Iceberg Writer).
