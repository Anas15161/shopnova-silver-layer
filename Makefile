.PHONY: help setup lint test format clean

VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
PYTEST = $(VENV)/bin/pytest
BLACK = $(VENV)/bin/black
ISORT = $(VENV)/bin/isort
FLAKE8 = $(VENV)/bin/flake8

help:
	@echo "Commandes disponibles pour ShopNova Silver Layer:"
	@echo "  make setup       - Créer le venv et installer les dépendances"
	@echo "  make format      - Formater le code avec Black et isort"
	@echo "  make lint        - Vérifier la qualité du code (Flake8, Black)"
	@echo "  make test        - Lancer la suite de tests unitaires"
	@echo "  make clean       - Nettoyer les caches Python et temporaires"

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

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov
