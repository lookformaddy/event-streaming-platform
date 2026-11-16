"""Demo entry point: run the 1,000-event end-to-end streaming demo."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from eventstream.demo import run_demo

if __name__ == "__main__":
    result = run_demo()
    raise SystemExit(0 if result["sli_status"] == "HEALTHY" else 1)
