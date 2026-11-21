"""Configuration globale des fixtures de test Pytest."""

from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Retourne le chemin absolu de la racine du projet."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def configs_dir(project_root: Path) -> Path:
    """Retourne le chemin vers le dossier des configurations."""
    return project_root / "configs"
