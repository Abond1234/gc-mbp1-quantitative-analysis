"""Resolve runtime paths independently of the caller's working directory."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def project_path(value: str | Path) -> Path:
    """Absolute explicit paths are retained; relative paths belong to this project."""
    path = Path(value)
    return path if path.is_absolute() else ROOT / path
