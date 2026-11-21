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
* Initialisation du repository Git et premier commit conventionnel.

---

## 2. Commandes Exécutées & Chronologie

### A. Création de l'arborescence et packages
```bash
cd /home/anas/Desktop/technologies-data-modernes/projet

# 1. Dossiers de configuration, données, docker, documentation, code source et tests
mkdir -p configs/schemas configs/quality_rules
mkdir -p data/bronze_sample data/quarantine data/warehouse
mkdir -p docker/spark docker/minio
mkdir -p docs/architecture
mkdir -p src/adapters src/schemas src/transformers src/quality src/storage src/orchestrator src/api src/utils
mkdir -p tests/unit tests/integration

# 2. Fichiers __init__.py pour faire de chaque sous-dossier un package Python valide
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

# 3. Fichiers .gitkeep pour tracker les répertoires vides
touch configs/schemas/.gitkeep configs/quality_rules/.gitkeep \
      docker/spark/.gitkeep docker/minio/.gitkeep docs/architecture/.gitkeep
```

### B. Environnement virtuel & Dépendances
```bash
# Création du venv
python3 -m venv .venv
source .venv/bin/activate

# Installation des dépendances applicatives et dev
pip install --upgrade pip
pip install -r requirements-dev.txt
```

### C. Validation de la qualité et Tests
```bash
# Vérification du style et du linting
black --check src tests
flake8 src tests --max-line-length=100 --extend-ignore=E203,W503

# Exécution des tests unitaires baseline
pytest
```
**Résultat des tests :**
```text
tests/unit/test_baseline.py::test_project_structure PASSED               [ 33%]
tests/unit/test_baseline.py::test_src_modules_exist PASSED               [ 66%]
tests/unit/test_baseline.py::test_env_example_exists PASSED              [100%]
============================== 3 passed in 0.02s ===============================
```

### D. Initialisation Git & Commit conventionnel
```bash
git init -b main
git add .
git commit -m "chore(init): setup project repository structure and baseline tooling"
```
**Résultat Git :**
```text
[main (root-commit) a2eb86a] chore(init): setup project repository structure and baseline tooling
31 files changed, 838 insertions(+)
```

---

## 3. Fichiers Créés & Justification

| Fichier | Emplacement | Rôle technique |
| :--- | :--- | :--- |
| `.gitignore` | `projet/.gitignore` | Exclut `.venv`, `data/`, fichiers parquet/csv/json, secrets `.env`, caches pytest et gros PDFs |
| `README.md` | `projet/README.md` | Présentation générale, architecture en 3 couches, prérequis et guide de démarrage |
| `pyproject.toml` | `projet/pyproject.toml` | Standardisation du style de code (Black 100 char, isort, configuration pytest) |
| `requirements.txt` | `projet/requirements.txt` | Dépendances applicatives (FastAPI, Pydantic, PyYAML, PyIceberg, PyArrow, Boto3, Loguru) |
| `requirements-dev.txt` | `projet/requirements-dev.txt` | Dépendances de développement et qualité (Pytest, Black, Flake8, isort) |
| `.env.example` | `projet/.env.example` | Modèle de configuration sans identifiants sensibles |
| `.env` | `projet/.env` | Instance locale des variables de configuration (ignoré par Git) |
| `Makefile` | `projet/Makefile` | Automatisation des commandes courantes (`make setup`, `make lint`, `make test`) |
| `tests/conftest.py` | `projet/tests/conftest.py` | Fixtures pytest (racine du projet, chemins de configuration) |
| `tests/unit/test_baseline.py` | `tests/unit/` | Tests d'intégrité de la structure du projet |

---

## 4. Structure Finale Obtenue

```
projet/
├── .env                      # Configuration locale (non versionnée)
├── .env.example              # Gabarit de configuration partagé
├── .gitignore                # Règles d'exclusion strictes
├── Makefile                  # Cibles make: setup, lint, format, test
├── pyproject.toml            # Config Black, isort, pytest
├── README.md                 # Documentation d'accueil
├── requirements.txt          # Dépendances coeur
├── requirements-dev.txt      # Dépendances dev/test
├── PHASE_0_EXECUTION.md      # Journal complet de cette Phase 0
├── configs/                  # Configurations déclaratives
│   ├── quality_rules/
│   └── schemas/
├── data/                     # Ignoré par Git
│   ├── bronze_sample/
│   ├── quarantine/
│   └── warehouse/
├── docker/                   # Composants conteneurs (Phase 1)
│   ├── minio/
│   └── spark/
├── docs/                     # Documentation et architecture
│   └── architecture/
├── src/                      # Modules Python du package
│   ├── adapters/
│   ├── api/
│   ├── orchestrator/
│   ├── quality/
│   ├── schemas/
│   ├── storage/
│   ├── transformers/
│   └── utils/
└── tests/                    # Tests unitaires et d'intégration
    ├── conftest.py
    ├── integration/
    └── unit/
        └── test_baseline.py
```

---

## 5. Prochaine Étape : PHASE 1 — Docker Infrastructure

Les fondations étant validées et versionnées, la prochaine étape consiste à déployer l'infrastructure conteneurisée :
1. **MinIO** : Serveur de stockage objet compatible S3 + provisionnement automatique des buckets `bronze`, `silver`, `quarantine`.
2. **Apache Spark** : Image Spark Master + Worker configurée avec les packages JAR Iceberg et Hadoop-AWS S3.
3. **Iceberg Catalog** : Déploiement et raccordement du catalogue Iceberg.
