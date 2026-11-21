"""Test unitaire baseline pour valider la structure et les configurations."""

from pathlib import Path


def test_project_structure(project_root: Path):
    """Vérifie que les répertoires obligatoires du projet existent."""
    required_dirs = [
        project_root / "src",
        project_root / "configs",
        project_root / "docker",
        project_root / "tests",
        project_root / "data",
    ]
    for directory in required_dirs:
        assert directory.exists(), f"Le dossier obligatoire {directory.name} est manquant."
        assert directory.is_dir(), f"{directory.name} doit être un répertoire."


def test_src_modules_exist(project_root: Path):
    """Vérifie que chaque module src possède un __init__.py valide."""
    modules = [
        "adapters",
        "schemas",
        "transformers",
        "quality",
        "storage",
        "orchestrator",
        "api",
        "utils",
    ]
    src_dir = project_root / "src"
    assert (src_dir / "__init__.py").exists()
    for mod in modules:
        mod_dir = src_dir / mod
        assert mod_dir.exists(), f"Module {mod} manquant dans src."
        assert (mod_dir / "__init__.py").exists(), f"__init__.py manquant dans src/{mod}."


def test_env_example_exists(project_root: Path):
    """Vérifie la présence du fichier .env.example."""
    env_example = project_root / ".env.example"
    assert env_example.exists()
    assert env_example.stat().st_size > 0
