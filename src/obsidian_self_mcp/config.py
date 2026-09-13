"""Configuration from environment variables. The database name has no default."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    couch_url: str = os.environ.get("OBSIDIAN_COUCH_URL", "") or os.environ.get("COUCHDB_URL", "")
    couch_user: str = os.environ.get("OBSIDIAN_COUCH_USER", "") or os.environ.get("COUCHDB_USER", "")
    couch_pass: str = os.environ.get("OBSIDIAN_COUCH_PASS", "") or os.environ.get("COUCHDB_PASSWORD", "")
    db_name: str = os.environ.get("OBSIDIAN_COUCH_DB", "") or os.environ.get("COUCHDB_DB", "")

    ntfy_url: str = os.environ.get("OBSIDIAN_NTFY_URL", "http://127.0.0.1:8080")
    ntfy_topic: str = os.environ.get("OBSIDIAN_NTFY_TOPIC", "obsidian-livesync")
    ntfy_batch_seconds: int = int(os.environ.get("OBSIDIAN_NTFY_BATCH_SECONDS", "60"))

    def __post_init__(self) -> None:
        # A wrong-but-reachable database answers every read as absent rather than
        # failing, so a missing name must stop here (SAI-DEC-203, SAI-OQ-128).
        if not self.db_name:
            raise ValueError(
                "No CouchDB database name: set OBSIDIAN_COUCH_DB or COUCHDB_DB. There is no default."
            )

    @property
    def db_url(self) -> str:
        return f"{self.couch_url}/{self.db_name}"
