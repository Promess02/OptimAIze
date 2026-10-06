"""Canonical filesystem paths for OptimAIze.

Resolves the repository root from this file's location so scripts, tests,
and services work regardless of the current working directory.

Environment overrides (useful in Docker):
  REPO_ROOT, DATA_DIR, DB_PATH, MODELS_DIR
"""

from __future__ import annotations

import os
from pathlib import Path

# shared/paths.py → repo root is one level up
_REPO_ROOT_DEFAULT = Path(__file__).resolve().parent.parent


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser().resolve() if value else default.resolve()


REPO_ROOT = _env_path("REPO_ROOT", _REPO_ROOT_DEFAULT)
DATA_DIR = _env_path("DATA_DIR", REPO_ROOT / "data")
MODELS_DIR = _env_path("MODELS_DIR", REPO_ROOT / "models")
SCRIPTS_DIR = REPO_ROOT / "scripts"
TEST_DIR = REPO_ROOT / "test"
DOCS_DIR = REPO_ROOT / "docs"
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"
PLAYGROUND_DIR = REPO_ROOT / "playground"

DB_PATH = _env_path("DB_PATH", DATA_DIR / "ecommerce.db")
SALES_CSV = DATA_DIR / "sales.csv"
INVENTORY_RECOMMENDATIONS_CSV = DATA_DIR / "inventory_recommendations.csv"
MERMAID_DIR = DOCS_DIR / "mermaid"


def ensure_data_dir() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR


def ensure_repo_on_syspath() -> Path:
    """Make `import shared...` work from scripts, tests, and notebooks."""
    import sys

    root = str(REPO_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    return REPO_ROOT


def resolve_repo_root_from(start: Path | None = None) -> Path:
    """Walk parents from *start* (or cwd) until shared/paths.py is found."""
    cur = (start or Path.cwd()).resolve()
    for candidate in [cur, *cur.parents]:
        if (candidate / "shared" / "paths.py").is_file():
            return candidate
    return REPO_ROOT
