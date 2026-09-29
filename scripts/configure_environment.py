"""Connect this project's own venv to its source; no source-project dependency."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if Path(sys.prefix).resolve() != (ROOT / ".venv").resolve():
    raise SystemExit("Run this script with the project's .venv interpreter")
target = Path(sys.prefix) / "Lib/site-packages/gc_mbp1_project.pth"
target.write_text("import sys; from pathlib import Path; sys.path.insert(0, str(Path(sys.prefix).resolve().parent))\n",
                  encoding="utf-8")
print("Configured project imports for", sys.executable)
