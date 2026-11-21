# Exécution de la PHASE 0 — Project Foundation

Ce document consigne l'ensemble des actions, configurations, créations de fichiers et validations réalisées pour la **Phase 0** du projet **ShopNova Silver Layer**.

---

## 1. Contexte & Objectifs

La Phase 0 a pour objectif de préparer un environnement de développement professionnel pour la **Silver Layer** du Data Lakehouse ShopNova :
* Définition de l'arborescence des répertoires conformément aux principes de séparation des responsabilités (*Separation of Concerns*).
* Mise en place de l'étanchéité Git via `.gitignore` pour éviter le commit involontaire de données, d'artefacts volumineux (PDF > 10 Mo) ou de secrets (`.env`).
* Création des fichiers de configuration standardisés (`pyproject.toml`, `requirements.txt`, `requirements-dev.txt`, `.env.example`, `Makefile`).
* Initialisation de l'environnement virtuel Python (`.venv`) et installation des dépendances.
* Implémentation d'une suite de tests baseline pour valider la conformité de l'arborescence.

---

## 2. Commandes Exécutées

### A. Création de l'arborescence
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# Dossiers de configuration, données, docker, documentation, code source et tests
mkdir -p configs/schemas configs/quality_rules
mkdir -p data/bronze_sample data/quarantine data/warehouse
mkdir -p docker/spark docker/minio
mkdir -p docs/architecture
mkdir -p src/adapters src/schemas src/transformers src/quality src/storage src/orchestrator src/api src/utils
mkdir -p tests/unit tests/integration

# Fichiers __init__.py pour faire de chaque répertoire un package Python valide
touch src/__init__.py \
      src/adapters/__init__.py \
      src/schemas/__init__.py \
      src/transformers/__init__.py \
      src/quality/__init__.py \
      src/storage/__init__.py \
      src/orchestrator/__init__.py \
      src/api/__init__.py \
      src/utils/__init__.py \
      tests/__init__.py \
      tests/unit/__init__.py \
      tests/integration/__init__.py
```

### B. Création de l'environnement virtuel
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-dev.txt
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `.gitignore` | `projet/.gitignore` | Exclut `.venv`, `data/`, fichiers parquet/csv/json, secrets `.env`, caches pytest et gros PDFs |
| `README.md` | `projet/README.md` | Présentation générale, architecture en 3 couches, prérequis et guide de démarrage |
| `pyproject.toml` | `projet/pyproject.toml` | Standardisation du style de code (Black 100 char, isort, configuration pytest) |
| `requirements.txt` | `projet/requirements.txt` | Dépendances applicatives (FastAPI, Pydantic, PyYAML, PyIceberg, Boto3, Loguru) |
| `requirements-dev.txt` | `projet/requirements-dev.txt` | Dépendances de développement et qualité (Pytest, Black, Flake8, isort) |
| `.env.example` | `projet/.env.example` | Modèle de configuration sans identifiants sensibles |
| `.env` | `projet/.env` | Instance locale des variables de configuration |
| `Makefile` | `projet/Makefile` | Automatisation des commandes courantes (`make setup`, `make lint`, `make test`) |
| `tests/conftest.py` | `projet/tests/conftest.py` | Fixtures pytest (racine du projet, chemins de configuration) |
| `tests/unit/test_baseline.py` | `tests/unit/` | Tests d'intégrité de la structure du projet |

---

## 4. Structure Obtenue

```
projet/
├── .env
├── .env.example
├── .gitignore
├── Makefile
├── pyproject.toml
├── README.md
├── requirements.txt
├── requirements-dev.txt
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
├── docs/
│   ├── architecture/
│   └── PHASE_0_FONDATIONS.md
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
    └── unit/
        └── test_baseline.py
```

---

## 5. Prochaine Étape : PHASE 1

Une fois la Phase 0 validée, la **Phase 1 (Docker Infrastructure)** consistera à :
1. Configurer `docker-compose.yml` avec MinIO, MinIO Client (mc) pour provisionner automatiquement les buckets `bronze`, `silver` et `quarantine`.
2. Déployer un cluster Spark (Master + Worker) avec les connecteurs JAR pour Apache Iceberg et S3 AWS.
3. Déployer et valider le catalogue Iceberg sélectionné (REST Catalog ou Nessie).
