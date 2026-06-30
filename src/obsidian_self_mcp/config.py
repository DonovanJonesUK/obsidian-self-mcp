"""Configuration from environment variables with sensible defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    couch_url: str = os.environ.get("OBSIDIAN_COUCH_URL", "") or os.environ.get("COUCHDB_URL", "")
    couch_user: str = os.environ.get("OBSIDIAN_COUCH_USER", "") or os.environ.get("COUCHDB_USER", "")
    couch_pass: str = os.environ.get("OBSIDIAN_COUCH_PASS", "") or os.environ.get("COUCHDB_PASSWORD", "")
    db_name: str = os.environ.get("OBSIDIAN_COUCH_DB", "") or os.environ.get("COUCHDB_DB", "obsidian-vault")

    ntfy_url: str = os.environ.get("OBSIDIAN_NTFY_URL", "http://127.0.0.1:8080")
    ntfy_topic: str = os.environ.get("OBSIDIAN_NTFY_TOPIC", "obsidian-livesync")
    ntfy_batch_seconds: int = int(os.environ.get("OBSIDIAN_NTFY_BATCH_SECONDS", "60"))

    @property
    def db_url(self) -> str:
        return f"{self.couch_url}/{self.db_name}"
