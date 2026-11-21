# ShopNova — E-Commerce Data Lakehouse (Silver Layer)

Système modulaire d'ingestion, de validation, de nettoyage, de contrôle qualité et de stockage de données E-commerce au format **Apache Iceberg**, exposé via une **API REST FastAPI**.

## 1. Contexte & Architecture

Le projet s'intègre dans une architecture de Data Lakehouse en 3 zones :
* **Bronze Layer** : Réception brute multi-format (CSV, JSON, Parquet).
* **Silver Layer (ce repository)** : Standardisation, déduplication, validation métier, gestion de quarantaine et persistance ACID avec Apache Iceberg sur MinIO (compatible S3).
* **Gold Layer** : Vues analytiques et agrégats décisionnels.

### Périmètre Silver Layer :
1. **Source Adapters** : Ingestion agnostique du format source (CSV, JSON, Parquet, Iceberg).
2. **Schema Management** : Contrats de schémas déclaratifs en YAML et type validation.
3. **PySpark Engine** : Transformations idempotentes (orders, customers, products, payments, order_items).
4. **Data Quality Framework** : Détection des anomalies, calcul des métriques et routage en quarantaine.
5. **Apache Iceberg Storage** : Gestion des snapshots, partitionnement et Time Travel sur MinIO.
6. **FastAPI REST Service** : Consultation haute performance des données Silver sans re-calcul Spark complet.

## 2. Structure du Projet

```
projet/
├── .env.example              # Variables d'environnement de référence
├── .gitignore                # Règles d'exclusion Git strictes
├── Makefile                  # Automatisation des tâches de développement
├── README.md                 # Documentation d'accueil
├── pyproject.toml            # Configuration des outils (Black, isort, pytest)
├── requirements.txt          # Dépendances applicatives de production
├── requirements-dev.txt      # Dépendances de développement et tests
├── configs/                  # Définitions déclaratives
│   ├── schemas/              # Schémas canoniques YAML
│   └── quality_rules/        # Règles de Data Quality YAML
├── data/                     # Répertoires de données locales (ignorés par Git)
│   ├── bronze_sample/        # Échantillons Bronze de test
│   ├── quarantine/           # Enregistrements rejetés
│   └── warehouse/            # Entrepôt Iceberg local
├── docker/                   # Définitions conteneurisées (Phase 1)
│   ├── spark/                # Dockerfile Spark + Connecteurs JAR Iceberg/S3
│   └── minio/                # Provisionnement buckets MinIO
├── docs/                     # Documentation d'architecture & rapports de phase
├── src/                      # Code source modulaire
│   ├── adapters/             # Phase 2 : Lecteurs multi-formats
│   ├── schemas/              # Phase 3 : Validation et mapping des schémas
│   ├── transformers/         # Phase 4 : Moteurs de transformation PySpark
│   ├── quality/              # Phase 5 : Framework Data Quality & Quarantaine
│   ├── storage/              # Phase 6 : Gestionnaire d'écriture Apache Iceberg
│   ├── orchestrator/         # Phase 7 : Pipeline runner & traçabilité
│   ├── api/                  # Phase 8 : API REST FastAPI
│   └── utils/                # Logging, configuration et sessions
└── tests/                    # Suite de tests automatisés
    ├── unit/                 # Tests unitaires
    └── integration/          # Tests end-to-end
```

## 3. Pile Technologique

* **Langage** : Python 3.10+
* **Traitement distribué** : Apache Spark 3.5.x & PySpark
* **Format de Table Ouvert** : Apache Iceberg 1.5+
* **Stockage Objet** : MinIO (S3-compatible)
* **API REST** : FastAPI, Uvicorn, Pydantic v2
* **Conteneurisation** : Docker, Docker Compose
* **Qualité & Tests** : Pytest, Black, Flake8
* **Gestion de Version** : Git (Conventional Commits)

## 4. Démarrage Rapide

### Prérequis
* Ubuntu 22.04 / 24.04 LTS
* Python 3.10+
* Docker Engine 24+ & Docker Compose v2+

### Installation du socle local
```bash
# 1. Cloner ou naviguer dans le dossier du projet
cd projet/

# 2. Initialiser le projet via Makefile
make setup

# 3. Lancer la vérification de qualité et les tests
make lint
make test
```

## 5. Conventions de Contribution

Commits conventionnels obligatoires :
* `feat(scope): ...` (nouvelle fonctionnalité)
* `fix(scope): ...` (correction de bug)
* `build(docker): ...` (infrastructure Docker, dépendances)
* `test(scope): ...` (ajout ou mise à jour de tests)
* `docs: ...` (mise à jour documentation)
* `chore: ...` (maintenance et configuration)
