import json
from pathlib import Path
from src.logger import get_logger

logger = get_logger()

CHECKPOINT_FILE = Path("data/raw/checkpoints.json")


class CheckpointManager:
    def __init__(self):
        self.checkpoints = self._load()

    def _load(self):
        if not CHECKPOINT_FILE.exists():
            logger.info("No checkpoint file found. Starting fresh.")
            return {}
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_last_id(self, channel_name):
        return self.checkpoints.get(channel_name, 0)

    def update(self, channel_name, last_id):
        self.checkpoints[channel_name] = last_id
        self._save()

    def _save(self):
        CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(self.checkpoints, f, indent=2)
