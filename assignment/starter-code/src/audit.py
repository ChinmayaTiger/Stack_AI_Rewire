"""Skill-driven and drift/quarantine logging."""

from pathlib import Path
from src import config

LOG_FILE = config.ROOT / "outputs" / "artifacts" / "pipeline_log.txt"


def log_event(entity: str, batch_id: str, message: str) -> None:
    """Append a log entry recording batch actions, metrics, and skill-driven events."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log_line = f"[{batch_id}] {entity}: {message}\n"
    print(log_line.strip())
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)
