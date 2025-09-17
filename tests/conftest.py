import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[0].parent
SERVICE_PATH = ROOT / "services" / "pii-classifier"
if SERVICE_PATH.exists():
    sys.path.insert(0, str(SERVICE_PATH))
