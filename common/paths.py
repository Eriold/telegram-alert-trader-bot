from pathlib import Path


def _resolve_data_root() -> Path:
    # Prefer project root "datos/" if it exists (current monorepo).
    project_root = Path(__file__).resolve().parents[2]
    candidate = project_root / "datos"
    if candidate.exists():
        return candidate
    # Otherwise, keep data local to alerts package.
    return Path(__file__).resolve().parents[1] / "datos"


DATA_DIR = _resolve_data_root()
DATA_DIR.mkdir(parents=True, exist_ok=True)
