# Exécution de la PHASE 1 — Docker Infrastructure

Ce document consigne l'ensemble des actions, configurations, créations de fichiers et validations réalisées pour la **Phase 1** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 1 a pour objectif de déployer une infrastructure locale conteneurisée, robuste et reproductible, dédiée au stockage Lakehouse et aux traitements distribués :
* Déploiement d'un stockage objet compatible S3 via **MinIO** avec persistance des données.
* Provisionnement automatisé et idempotent des buckets du Data Lake (`bronze`, `silver`, `quarantine`, `warehouse`).
* Déploiement du **Catalogue Apache Iceberg REST** conforme aux standards modernes OpenAPI.
* Déploiement du cluster **Apache Spark 3.5** pré-équipé du runtime Iceberg et des extensions AWS S3 (`hadoop-aws`, `aws-java-sdk-bundle`).
* Configuration du catalogue par défaut `lake` et des accès S3A dans `spark-defaults.conf`.
* Validation des communications inter-services via un test PySpark d'écriture et de lecture ACID de bout en bout.

---

## 2. Commandes Exécutées & Chronologie

### A. Démarrage de la pile Docker Compose
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# Démarrage des conteneurs avec gestion de l'API Docker Desktop
DOCKER_API_VERSION=1.45 docker compose up -d
```

### B. Contrôle de l'état des conteneurs
```bash
docker ps
```
**Résultat :**
```text
CONTAINER ID   IMAGE                                STATUS                    PORTS
1a7b36e0d8a4   tabulario/spark-iceberg:latest       Up (healthy)              0.0.0.0:8080->8080/tcp, 0.0.0.0:8888->8888/tcp, 0.0.0.0:4040->4040/tcp
98e1034d9cd9   apache/iceberg-rest-fixture:latest   Up (healthy)              0.0.0.0:8181->8181/tcp
edd847d336f6   pgsty/minio:latest                   Up (healthy)              0.0.0.0:9000-9001->9000-9001/tcp
```

### C. Vérification de la création des buckets MinIO
```bash
docker logs shopnova-mc-init
```
**Résultat :**
```text
MinIO prêt. Création des buckets...
Bucket created successfully `local/bronze`.
Bucket created successfully `local/silver`.
Bucket created successfully `local/quarantine`.
Bucket created successfully `local/warehouse`.
Access permission for `local/bronze` is set to `download`
Buckets créés avec succès :
[UTC]     0B bronze/
[UTC]     0B quarantine/
[UTC]     0B silver/
[UTC]     0B warehouse/
```

### D. Exécution du test de validation PySpark E2E
```bash
docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_infrastructure.py
```
**Résultat du test PySpark dans Spark :**
```text
============================================================
DEMARRAGE DU TEST D'INFRASTRUCTURE SHOPNOVA
============================================================
[1/5] SparkSession initialisée avec succès.
[2/5] Création du namespace test dans le catalogue REST 'lake'...
[3/5] Création de la table Iceberg lake.infra_check.healthcheck...
[4/5] Insertion de données de test (test d'écriture S3 / MinIO)...
[5/5] Relecture des données depuis le catalogue Iceberg...
+--------+----------------------------+-------+------------------------+
|check_id|service_name                |status |checked_at              |
+--------+----------------------------+-------+------------------------+
|1       |MinIO + Iceberg REST + Spark|HEALTHY|2026-11-21 17:18:10.4425|
+--------+----------------------------+-------+------------------------+

============================================================
✅ TEST D'INFRASTRUCTURE REUSSI AVEC SUCCES !
MinIO S3, Iceberg REST Catalog et Spark fonctionnent de concert.
============================================================
```

### E. Tests d'intégration réseau sur l'hôte (Pytest)
```bash
source .venv/bin/activate
pytest tests/integration/test_infrastructure.py
```
**Résultat des tests :**
```text
tests/integration/test_infrastructure.py::test_minio_health PASSED             [ 33%]
tests/integration/test_infrastructure.py::test_iceberg_rest_catalog_config PASSED [ 66%]
tests/integration/test_infrastructure.py::test_minio_buckets_exist PASSED       [100%]
============================== 3 passed in 0.24s ===============================
```

### F. Enregistrement Git & Commit conventionnel
```bash
git add Makefile docker-compose.yml docker/spark/conf/ docker/spark/scripts/ docs/PHASE_1_INFRASTRUCTURE.md tests/integration/test_infrastructure.py
git commit -m "build(docker): deploy and validate minio, iceberg rest catalog and spark infrastructure"
```
**Résultat Git :**
```text
[main 5e7c780] build(docker): deploy and validate minio, iceberg rest catalog and spark infrastructure
 6 files changed, 419 insertions(+), 6 deletions(-)
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `docker-compose.yml` | `projet/docker-compose.yml` | Définition des 4 services : MinIO, MinIO Client (mc), Iceberg REST Catalog, Spark Master/Worker |
| `spark-defaults.conf` | `docker/spark/conf/` | Configuration du catalogue REST `lake`, extensions Iceberg et connecteurs S3A Hadoop |
| `test_infrastructure.py` | `docker/spark/scripts/` | Script PySpark testant la création de namespace, de table Iceberg, insertion et relecture |
| `test_infrastructure.py` | `tests/integration/` | Tests d'intégration Pytest vérifiant la santé de MinIO, des buckets et du catalogue REST |
| `PHASE_1_INFRASTRUCTURE.md` | `docs/` | Rapport technique détaillé de l'infrastructure Docker et comparatif des catalogues |
| `Makefile` | `projet/Makefile` | Cibles `docker-up`, `docker-down`, `docker-test`, `docker-logs` ajoutées |

---

## 4. Structure Finale Obtenue

```
projet/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml         # Définition des conteneurs d'infrastructure
├── Makefile                  # Orchestration enrichie (docker-up, docker-test, etc.)
├── pyproject.toml
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── PHASE_0_EXECUTION.md      # Journal de la Phase 0
├── PHASE_1_EXECUTION.md      # Journal de la Phase 1 (ce fichier)
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
│       │   └── spark-defaults.conf    # Injection configuration Iceberg & S3A
│       └── scripts/
│           └── test_infrastructure.py # Script de validation PySpark
├── docs/
│   ├── architecture/
│   └── PHASE_1_INFRASTRUCTURE.md
├── src/
│   ├── adapters/
│   ├── api/
│   ├── orchestrator/
│   ├── quality/
│   ├── schemas/
│   ├── storage/
│   ├── transformers/
│   └── utils/
└── tests/
    ├── conftest.py
    ├── integration/
    │   └── test_infrastructure.py     # Tests d'intégration réseau MinIO & Catalog
    └── unit/
        └── test_baseline.py
```

---

## 5. Prochaine Étape : PHASE 2 — Source Adapters

L'infrastructure étant active et validée, la prochaine étape consiste à construire le moteur d'ingestion modulaire :
1. Définition du contrat `BaseSourceAdapter`.
2. Implémentation des adaptateurs spécialisés : **CSV**, **JSON** (CDC Debezium), **Parquet** et **Iceberg**.
3. Conception de la fabrique `SourceAdapterFactory` pour abstraire totalement la provenance et le format des données Bronze.
