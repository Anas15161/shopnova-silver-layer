.PHONY: help setup lint test format clean docker-up docker-down docker-test test-adapters test-schemas test-transformers docker-logs

VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
PYTEST = $(VENV)/bin/pytest
BLACK = $(VENV)/bin/black
ISORT = $(VENV)/bin/isort
FLAKE8 = $(VENV)/bin/flake8
DOCKER_COMPOSE = DOCKER_API_VERSION=1.45 docker compose

help:
	@echo "Commandes disponibles pour ShopNova Silver Layer:"
	@echo "  make setup            - Créer le venv et installer les dépendances"
	@echo "  make format           - Formater le code avec Black et isort"
	@echo "  make lint             - Vérifier la qualité du code (Flake8, Black)"
	@echo "  make test             - Lancer la suite de tests unitaires et intégration hôte"
	@echo "  make docker-up        - Démarrer l'infrastructure Docker (MinIO, REST Catalog, Spark)"
	@echo "  make docker-down      - Arrêter les conteneurs Docker"
	@echo "  make docker-test      - Exécuter le test de santé Spark + Iceberg + S3"
	@echo "  make test-adapters    - Exécuter les tests E2E des adaptateurs sources"
	@echo "  make test-schemas     - Exécuter les tests E2E de validation et mapping de schémas"
	@echo "  make test-transformers- Exécuter les tests E2E du moteur de transformation et déduplication"
	@echo "  make docker-logs      - Afficher les logs des services Docker"
	@echo "  make clean            - Nettoyer les caches Python et temporaires"

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	@if [ ! -f .env ]; then cp .env.example .env; echo ".env créé depuis .env.example"; fi

format:
	$(ISORT) src tests
	$(BLACK) src tests

lint:
	$(BLACK) --check src tests
	$(FLAKE8) src tests --max-line-length=100 --extend-ignore=E203,W503

test:
	$(PYTEST) tests/

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-test:
	docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_infrastructure.py

test-adapters:
	docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_adapters_e2e.py

test-schemas:
	docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_schema_management_e2e.py

test-transformers:
	docker exec shopnova-spark-iceberg python3 /home/iceberg/scripts/test_transformers_e2e.py

docker-logs:
	$(DOCKER_COMPOSE) logs -f

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov
