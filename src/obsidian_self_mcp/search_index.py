"""A per-note SQLite full-text index for `search_notes`.

`search_notes` used to regex-scan every chunk document in CouchDB. Mango cannot
index a regex, so each query cost a pass over the whole database, and by
2026-09-29 a rare term took about 50 s against a 30 s client timeout
(SAI-TKT-00R). This module holds one row per note, with the note's text
reassembled from its chunks, in an FTS5 table using the trigram tokenizer. That
keeps today's matching semantics: case-insensitive substring, so `fair` still
finds `fairness`.

Only the extensions in OBSIDIAN_SEARCH_INDEX_EXTS are indexed (default `.md`,
`.canvas`, `.base`, `.txt`, SAI-DEC-239). Base64 attachments and plugin bundles
matched short queries by chance and would multiply the index size.

A note with a chunk missing from CouchDB is recorded as `failed` and its text
is never indexed from the chunks that do exist: a gap reassembled as empty text
is a plausible wrong answer, not a caught error (SAI-OQ-131, the same rule as
the backlink scan and PAI's vault-batch.ts).

The index is one SQLite file per database, so the dev server can never read
production's. It is written only by this module; CouchDB is only ever read.

Usage:
    python -m obsidian_self_mcp.search_index build [--out PATH]
    python -m obsidian_self_mcp.search_index stats [--out PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sqlite3
import sys
import time
from pathlib import Path

from .client import ObsidianVaultClient
from .config import Config

SCHEMA_VERSION = "1"
DEFAULT_EXTS = "md,canvas,base,txt"
# Notes per chunk-fetch round. Bounds memory to one group's text at a time.
BUILD_GROUP = 200


def indexed_exts() -> frozenset[str]:
    raw = os.environ.get("OBSIDIAN_SEARCH_INDEX_EXTS", DEFAULT_EXTS)
    return frozenset(e.strip().lower().lstrip(".") for e in raw.split(",") if e.strip())


def ext_of(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def default_index_path(db_name: str) -> Path:
    base = os.environ.get("OBSIDIAN_SEARCH_INDEX_DIR") or os.path.join(
        os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state"),
        "obsidian-self-mcp",
    )
    return Path(base) / f"search-{db_name}.sqlite"


def children_key(doc: dict) -> str:
    return hashlib.sha1("\0".join(doc.get("children", [])).encode()).hexdigest()


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    # The FTS table stores each ok note's text itself; a failed note has a
    # `notes` row and no FTS row. External content (text in a plain table, FTS
    # over it) measured the same size on production, 2026-09-29: 176.7 MB
    # against 176.6 MB. It was rejected because every delete must then quote
    # the exact old text, and a wrong quote desyncs the index silently.
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS notes (
          rowid INTEGER PRIMARY KEY,
          id TEXT NOT NULL UNIQUE,
          path TEXT NOT NULL,
          path_lc TEXT NOT NULL,
          ext TEXT NOT NULL,
          rev TEXT NOT NULL,
          children_key TEXT NOT NULL,
          mtime INTEGER,
          status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
          reason TEXT
        );
        CREATE INDEX IF NOT EXISTS notes_path_lc ON notes(path_lc);
        CREATE INDEX IF NOT EXISTS notes_status ON notes(status);
        CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
          body, tokenize='trigram'
        );
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )


def set_meta(conn: sqlite3.Connection, **values) -> None:
    conn.executemany(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [(k, str(v)) for k, v in values.items()],
    )


def get_meta(conn: sqlite3.Connection) -> dict[str, str]:
    return dict(conn.execute("SELECT key, value FROM meta"))


def remove_note(conn: sqlite3.Connection, doc_id: str) -> None:
    """Drop a note's row and its FTS row, if any."""
    row = conn.execute("SELECT rowid FROM notes WHERE id = ?", (doc_id,)).fetchone()
    if row is None:
        return
    conn.execute("DELETE FROM notes_fts WHERE rowid = ?", (row[0],))
    conn.execute("DELETE FROM notes WHERE rowid = ?", (row[0],))


def put_note(
    conn: sqlite3.Connection, doc: dict, chunks: dict[str, str]
) -> str:
    """Replace one note's row from its file doc and fetched chunks.

    Returns 'ok' or 'failed'. Idempotent: replaying the same doc leaves the same
    row, which is what makes a replayed change-feed sequence safe.
    """
    doc_id = doc["_id"]
    path = doc.get("path", doc_id)
    children = doc.get("children", [])
    missing = [c for c in children if c not in chunks]
    remove_note(conn, doc_id)
    common = (
        doc_id, path, path.lower(), ext_of(path), doc.get("_rev", ""),
        children_key(doc), doc.get("mtime"),
    )
    if missing:
        conn.execute(
            "INSERT INTO notes(id, path, path_lc, ext, rev, children_key, mtime,"
            " status, reason) VALUES(?, ?, ?, ?, ?, ?, ?, 'failed', ?)",
            (*common, f"{len(missing)} of {len(children)} chunks missing"),
        )
        return "failed"
    body = "".join(chunks[c] for c in children)
    cur = conn.execute(
        "INSERT INTO notes(id, path, path_lc, ext, rev, children_key, mtime,"
        " status, reason) VALUES(?, ?, ?, ?, ?, ?, ?, 'ok', NULL)",
        common,
    )
    conn.execute(
        "INSERT INTO notes_fts(rowid, body) VALUES(?, ?)", (cur.lastrowid, body)
    )
    return "ok"


async def _update_seq(client: ObsidianVaultClient) -> str:
    http = await client._get_client()
    resp = await http.get("")
    resp.raise_for_status()
    return str(resp.json()["update_seq"])


async def build(client: ObsidianVaultClient, out: Path) -> dict:
    """Build a fresh index into `out`, atomically replacing any existing file.

    The sequence is read before the listing, so a change landing during the
    build is replayed by the follower rather than lost; replay is idempotent.
    """
    started = time.monotonic()
    seq = await _update_seq(client)
    exts = indexed_exts()
    docs = [
        d for d in await client._get_all_file_docs()
        if ext_of(d.get("path", d["_id"])) in exts
    ]

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".building")
    for p in (tmp, Path(str(tmp) + "-wal"), Path(str(tmp) + "-shm")):
        p.unlink(missing_ok=True)
    conn = connect(tmp)
    counts = {"ok": 0, "failed": 0}
    try:
        create_schema(conn)
        for i in range(0, len(docs), BUILD_GROUP):
            group = docs[i : i + BUILD_GROUP]
            ids = [c for d in group for c in d.get("children", [])]
            chunks = await client._fetch_chunks_batched(ids) if ids else {}
            with conn:
                for d in group:
                    counts[put_note(conn, d, chunks)] += 1
        with conn:
            set_meta(
                conn,
                schema_version=SCHEMA_VERSION,
                db_name=client.config.db_name,
                last_seq=seq,
                built_at=int(time.time()),
                heartbeat_at=int(time.time()),
                note_count=counts["ok"],
                failed_count=counts["failed"],
                exts=",".join(sorted(exts)),
            )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    os.replace(tmp, out)
    for suffix in ("-wal", "-shm"):
        Path(str(tmp) + suffix).unlink(missing_ok=True)
    return {
        **counts,
        "listed": len(docs),
        "elapsed_s": round(time.monotonic() - started, 1),
        "bytes": out.stat().st_size,
    }


def stats(path: Path) -> dict:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        meta = get_meta(conn)
        by_status = dict(conn.execute("SELECT status, COUNT(*) FROM notes GROUP BY status"))
        by_ext = dict(conn.execute("SELECT ext, COUNT(*) FROM notes GROUP BY ext"))
    finally:
        conn.close()
    return {"path": str(path), "bytes": path.stat().st_size, "meta": meta,
            "status": by_status, "ext": by_ext}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="python -m obsidian_self_mcp.search_index")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "stats"):
        p = sub.add_parser(name)
        p.add_argument("--out", type=Path, help="index file (default: per-database state path)")
    args = ap.parse_args(argv)

    config = Config()
    out = args.out or default_index_path(config.db_name)
    if args.cmd == "stats":
        for k, v in stats(out).items():
            print(f"{k}: {v}")
        return

    async def run() -> dict:
        client = ObsidianVaultClient(config)
        try:
            return await build(client, out)
        finally:
            await client.close()

    result = asyncio.run(run())
    print(f"built {out}: {result}")


if __name__ == "__main__":
    sys.exit(main())
