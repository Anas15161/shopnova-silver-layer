# PHASE 1 — Docker Infrastructure (MinIO S3, Iceberg Catalog & Spark Cluster)

Ce document consigne la conception, les choix d'architecture, la configuration des services et la validation de la **Phase 1** du projet **ShopNova Silver Layer**.

---

## 1. Objectif

Déployer un environnement Lakehouse local, entièrement conteneurisé, reproductible et prêt pour les traitements PySpark et le stockage ACID Iceberg :
* Déploiement d'un stockage objet compatible S3 via **MinIO**.
* Provisionnement automatique des buckets du Data Lake (`bronze`, `silver`, `quarantine`, `warehouse`).
* Déploiement du **Catalogue Apache Iceberg REST**.
* Déploiement de l'environnement d'exécution **Apache Spark 3.5** pré-configuré avec les extensions Iceberg et les connecteurs S3 (`aws-bundle`, `hadoop-aws`).
* Validation des communications inter-services et exécution d'un test end-to-end (création de namespace, création de table Iceberg, écriture S3, relecture).

---

## 2. Comparaison et Décisions Techniques

### A. Choix du Catalogue Apache Iceberg

| Solution | Type d'architecture | Avantages | Inconvénients / Limites | Décision |
| :--- | :--- | :--- | :--- | :--- |
| **Hadoop Catalog** | Fichiers S3 directs | Zéro service additionnel, ultra simple | **Pas de locking atomique ACID fiable sur S3** sans DynamoDB. Risque de corruption lors d'écritures concurrentes. Déconseillé en production | ❌ Rejeté |
| **Hive Metastore (HMS)** | Service Thrift + RDBMS | Historiquement répandu dans l'écosystème Hadoop | Lourd, requiert un conteneur PostgreSQL + un démon Java HMS dédié, lent au démarrage, daté pour les Lakehouses modernes | ❌ Rejeté |
| **Project Nessie** | Serveur REST + Git-like versioning | Branches, tags et commits sur les données | Complexité conceptuelle et de gouvernance supérieure non requise pour le périmètre Silver CDC | ❌ Non retenu pour le MVP |
| **Apache Iceberg REST Catalog** | Spécification officielle REST OpenAPI | **Standard moderne officiel**, léger, sans état lourd, interopérable nativement avec Spark, PyIceberg (FastAPI) et DuckDB | Nécessite un service léger HTTP (fourni par `apache/iceberg-rest-fixture`) | ✅ **Retenu** |

### B. Moteur d'exécution Spark
* **Image retenue** : `tabulario/spark-iceberg:latest`.
* **Justification** : Embarque Apache Spark 3.5, Java 17, PySpark, ainsi que les dépendances critiques (`iceberg-spark-runtime-3.5_2.12`, `hadoop-aws`, `aws-java-sdk-bundle`) pré-compilées. Évite de télécharger des gigaoctets de JARs Maven à chaque instanciation de conteneur.

---

## 3. Architecture des Services Docker

```
                                    ┌────────────────────────┐
                                    │      Host / Host OS    │
                                    │  (FastAPI / Pytest)    │
                                    └───────────┬────────────┘
                                                │ Ports exposés (8181, 9000, 9001, 8080, 8888)
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        DOCKER NETWORK: shopnova_network                                │
│                                                                                        │
│   ┌───────────────────────────────┐           ┌────────────────────────────────────┐   │
│   │       shopnova-minio          │           │       shopnova-iceberg-rest        │   │
│   │  (S3 Storage API :9000)       │◀──────────│ (Catalog API :8181 - OpenAPI Spec) │   │
│   │  (Web Console UI  :9001)      │           └─────────────────┬──────────────────┘   │
│   └───────────────▲───────────────┘                             │                      │
│                   │ S3A read/write                              │ Catalog Metadata     │
│                   │                                             ▼                      │
│   ┌───────────────┴───────────────┐           ┌────────────────────────────────────┐   │
│   │       shopnova-mc-init        │           │      shopnova-spark-iceberg        │   │
│   │ (Provisioning automatique:    │           │ (PySpark 3.5 + Iceberg Runtime)    │   │
│   │  bronze, silver, quarantine,  │           │ UI Spark :8080 / Jupyter :8888     │   │
│   │  warehouse)                   │           │ Catalogue par défaut: 'lake'       │   │
│   └───────────────────────────────┘           └────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Configuration des Fichiers

### A. [docker-compose.yml](file:///home/anas/Desktop/technologies-data-modernes/projet/docker-compose.yml)
Définit les 4 services avec healthchecks, dépendances ordonnées et volumes persistants.

### B. [docker/spark/conf/spark-defaults.conf](file:///home/anas/Desktop/technologies-data-modernes/projet/docker/spark/conf/spark-defaults.conf)
Injecte la configuration Spark suivante :
* `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
* `spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog`
* `spark.sql.catalog.lake.type=rest`
* `spark.sql.catalog.lake.uri=http://rest:8181`
* `spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
* `spark.sql.catalog.lake.warehouse=s3://warehouse/`
* `spark.sql.catalog.lake.s3.endpoint=http://minio:9000`
* `spark.sql.defaultCatalog=lake`
* Configuration `fs.s3a` pour la compatibilité avec `spark.read.json("s3a://bronze/...")`.

---

## 5. Commandes d'Exécution

```bash
# 1. Démarrer l'infrastructure
make docker-up
# ou : DOCKER_API_VERSION=1.45 docker compose up -d

# 2. Vérifier les conteneurs actifs
docker ps

# 3. Exécuter le test de validation Spark + Iceberg + MinIO
make docker-test

# 4. Lancer la suite de tests complète (Unitaires + Intégration hôte)
make test
```

---

## 6. Résultats Obtenus

### Statut des Conteneurs
```text
CONTAINER ID   IMAGE                                STATUS                    PORTS
1a7b36e0d8a4   tabulario/spark-iceberg:latest       Up (healthy)              0.0.0.0:8080->8080/tcp, 0.0.0.0:8888->8888/tcp
98e1034d9cd9   apache/iceberg-rest-fixture:latest   Up (healthy)              0.0.0.0:8181->8181/tcp
edd847d336f6   pgsty/minio:latest                   Up (healthy)              0.0.0.0:9000-9001->9000-9001/tcp
```

### Buckets MinIO initialisés
```text
[UTC]     0B bronze/
[UTC]     0B quarantine/
[UTC]     0B silver/
[UTC]     0B warehouse/
```

### Test Spark + Iceberg End-to-End (`test_infrastructure.py`)
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

### Tests Pytest (6/6 réussis)
```text
tests/integration/test_infrastructure.py::test_minio_health PASSED             [ 16%]
tests/integration/test_infrastructure.py::test_iceberg_rest_catalog_config PASSED [ 33%]
tests/integration/test_infrastructure.py::test_minio_buckets_exist PASSED       [ 50%]
tests/unit/test_baseline.py::test_project_structure PASSED                     [ 66%]
tests/unit/test_baseline.py::test_src_modules_exist PASSED                     [ 83%]
tests/unit/test_baseline.py::test_env_example_exists PASSED                    [100%]
============================== 6 passed in 0.26s ===============================
```

---

## 7. Diagnostic & Astuces d'Ingénierie

1. **Erreur d'API Docker Desktop (`500 Internal Server Error / v1.54`)** :
   * *Diagnostic* : Le client Docker sous Ubuntu négocie la version API 1.54, alors que le daemon Docker Desktop attend une version stable compatible 1.45.
   * *Solution* : Exporter `DOCKER_API_VERSION=1.45` (intégré directement dans le `Makefile`).
2. **Résolution DNS `UnknownHostException: rest` dans Spark** :
   * *Diagnostic* : Par défaut, Spark cherche l'hôte `rest` sur le port 8181.
   * *Solution* : Configuration de l'alias réseau `rest` sur le service `iceberg-rest` dans le fichier `docker-compose.yml`.
