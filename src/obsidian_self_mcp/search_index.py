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

`follow` keeps the index current from CouchDB's `_changes` feed, resuming from
the sequence the last build or cycle committed. It is the only writer while it
runs; the MCP server reads the same file concurrently in WAL mode, so each
cycle commits one short transaction.

Usage:
    python -m obsidian_self_mcp.search_index build [--out PATH]
    python -m obsidian_self_mcp.search_index stats [--out PATH]
    python -m obsidian_self_mcp.search_index follow [--out PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import contextlib
import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

import httpx

from .client import ObsidianVaultClient
from .config import Config
from .search_fold import FOLD_VERSION, has_fold_odd

# 2: fold_odd column, newnote bodies decoded, couch_origin and pending in meta.
SCHEMA_VERSION = "2"
DEFAULT_EXTS = "md,canvas,base,txt"
# Notes per chunk-fetch round. Bounds memory to one group's text at a time.
BUILD_GROUP = 200
# Change rows per `_changes` request, so one cycle's transaction stays short.
CHANGES_LIMIT = 500
# CouchDB holds a longpoll open for up to 60 s; the request timeout must sit
# comfortably above that or every quiet minute would read as a transport error.
CHANGES_LONGPOLL_MS = 60000
CHANGES_HTTP_TIMEOUT = 90.0
# Doc ids per keyed `_all_docs` POST when re-reading failed notes.
RETRY_READ_BATCH = 400
# A failed note is retried every cycle for this long after it first failed...
RETRY_FAST_WINDOW = 600
# ...and then at most this often, unless one of its missing chunks arrives.
RETRY_SLOW_INTERVAL = 3600
# Failed notes re-read per cycle at most, so a build that leaves many notes
# failed cannot stretch one cycle past the heartbeat threshold.
RETRY_MAX_PER_CYCLE = 200
BACKOFF_CAP = 60.0
# Ids that are never notes: LiveSync chunks, its index docs, design docs.
NOT_NOTE_PREFIXES = ("h:", "ix:", "_design/")


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


class IndexBusy(Exception):
    """Another process holds the index file; building now would race it."""


def doc_path(doc: dict) -> str:
    """The doc's path, or its id when `path` is missing or not a string."""
    path = doc.get("path")
    return path if isinstance(path, str) else doc["_id"]


def couch_origin(couch_url: str) -> str:
    """scheme://host:port of the CouchDB server, without credentials or path."""
    url = httpx.URL(couch_url)
    return f"{url.scheme}://{url.host}:{url.port or (443 if url.scheme == 'https' else 80)}"


def chunk_ids(doc: dict) -> list[str] | None:
    """The doc's chunk ids, or None when `children` is not a list of strings."""
    ids = doc.get("children")
    if isinstance(ids, list) and all(isinstance(c, str) for c in ids):
        return ids
    return None


def children_key(doc: dict) -> str:
    ids = chunk_ids(doc)
    if ids is None:
        return "malformed"
    return hashlib.sha1("\0".join(ids).encode()).hexdigest()


@contextlib.contextmanager
def index_lock(out: Path):
    """Hold an exclusive lock on `out` for as long as this process writes it.

    The follower holds it for its lifetime and a CLI build takes it, so a build
    can never replace the file under a running follower: the follower would keep
    writing its old WAL, which by path shadows the new file for every reader.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(f"{out}.lock", os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise IndexBusy(
                f"{out} is held by another process (the follower service?); stop it first"
            ) from None
        yield
    finally:
        os.close(fd)


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
          reason TEXT,
          fold_odd INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS notes_path_lc ON notes(path_lc);
        CREATE INDEX IF NOT EXISTS notes_status ON notes(status);
        CREATE INDEX IF NOT EXISTS notes_fold_odd ON notes(fold_odd) WHERE fold_odd = 1;
        CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
          body, tokenize='trigram'
        );
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS retry (
          id TEXT PRIMARY KEY,
          first_failed_at INTEGER NOT NULL,
          last_tried_at INTEGER NOT NULL,
          missing TEXT NOT NULL
        );
        """
    )
    # `retry` holds follower state for failed notes: when each first failed,
    # when it was last tried, and which chunk ids were missing (a JSON list).
    # A failed note with no row (failed during build) is due at once.


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

    Never raises on the doc's shape. A doc whose fields are malformed becomes a
    failed row with the reason, because one bad doc raising here would crash
    the follower and systemd would restart it into the same doc forever.
    A `newnote` doc holds base64; its decoded text is what gets indexed.
    """
    doc_id = doc["_id"]
    path = doc.get("path", doc_id)
    reason = None
    if not isinstance(path, str):
        path, reason = doc_id, "malformed doc: path is not a string"
    rev, mtime = doc.get("_rev", ""), doc.get("mtime")
    remove_note(conn, doc_id)
    common = (
        doc_id, path, path.lower(), ext_of(path), rev if isinstance(rev, str) else str(rev),
        children_key(doc), mtime if isinstance(mtime, int) else None,
    )
    ids = chunk_ids(doc)
    body = ""
    if reason:
        pass
    elif ids is None:
        reason = "malformed doc: children is not a list of chunk ids"
    elif missing := [c for c in ids if c not in chunks]:
        reason = f"{len(missing)} of {len(ids)} chunks missing"
    elif bad := [c for c in ids if not isinstance(chunks[c], str)]:
        reason = f"malformed chunk: {len(bad)} of {len(ids)} chunks hold no text"
    elif doc.get("type") == "newnote":
        decoded = decode_newnote([chunks[c] for c in ids])
        if decoded is None:
            reason = "binary content is not base64-encoded UTF-8 text"
        else:
            body = decoded
    else:
        body = "".join(chunks[c] for c in ids)
    if reason:
        conn.execute(
            "INSERT INTO notes(id, path, path_lc, ext, rev, children_key, mtime,"
            " status, reason) VALUES(?, ?, ?, ?, ?, ?, ?, 'failed', ?)",
            (*common, reason),
        )
        return "failed"
    cur = conn.execute(
        "INSERT INTO notes(id, path, path_lc, ext, rev, children_key, mtime,"
        " status, reason, fold_odd) VALUES(?, ?, ?, ?, ?, ?, ?, 'ok', NULL, ?)",
        (*common, int(has_fold_odd(body))),
    )
    conn.execute(
        "INSERT INTO notes_fts(rowid, body) VALUES(?, ?)", (cur.lastrowid, body)
    )
    return "ok"


def decode_newnote(parts: list[str]) -> str | None:
    """Decode a `newnote` doc's chunks to UTF-8 text, or None if they are not that.

    This client's writer slices one base64 string across chunks, so the joined
    text decodes. A writer that encodes each piece separately leaves padding
    mid-stream, which only decodes piece by piece; both are tried. Every
    production Base was a single chunk on 2026-10-01, so the second form is
    covered by a test, not by observed data.
    """
    try:
        return base64.b64decode("".join(parts), validate=True).decode("utf-8")
    except (binascii.Error, ValueError):
        pass
    try:
        return b"".join(base64.b64decode(p, validate=True) for p in parts).decode("utf-8")
    except (binascii.Error, ValueError):
        return None


async def fetch_pending(client: ObsidianVaultClient, since: str) -> int:
    """How many changes CouchDB holds after `since`."""
    http = await client._get_client()
    resp = await http.get("/_changes", params={"since": since, "limit": 1})
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data.get("pending"), int) or not isinstance(data.get("results"), list):
        raise RuntimeError(f"_changes response has no pending count: {str(data)[:200]}")
    return data["pending"] + len(data["results"])


async def _update_seq(client: ObsidianVaultClient) -> str:
    http = await client._get_client()
    resp = await http.get("")
    resp.raise_for_status()
    return str(resp.json()["update_seq"])


async def build(client: ObsidianVaultClient, out: Path) -> dict:
    """Build a fresh index into `out`, atomically replacing any existing file.

    The caller must hold `index_lock(out)`. The sequence is read before the
    listing, so a change landing during the build is replayed by the follower
    rather than lost; replay is idempotent.
    """
    started = time.monotonic()
    seq = await _update_seq(client)
    exts = indexed_exts()
    docs = [
        d for d in await client._get_all_file_docs()
        if ext_of(doc_path(d)) in exts
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
            ids = [c for d in group for c in (chunk_ids(d) or [])]
            chunks = await client._fetch_chunks_batched(ids) if ids else {}
            with conn:
                for d in group:
                    counts[put_note(conn, d, chunks)] += 1
        # The changes that landed during the build, so a reader sees this lag
        # rather than the fresh heartbeat alone.
        pending = await fetch_pending(client, seq)
        with conn:
            set_meta(
                conn,
                schema_version=SCHEMA_VERSION,
                fold_version=FOLD_VERSION,
                db_name=client.config.db_name,
                couch_origin=couch_origin(client.config.couch_url),
                last_seq=seq,
                pending=pending,
                built_at=int(time.time()),
                heartbeat_at=int(time.time()),
                note_count=counts["ok"],
                failed_count=counts["failed"],
                exts=",".join(sorted(exts)),
            )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    # An old WAL beside `out` belongs to the file being replaced, but SQLite
    # finds a WAL by path: left in place it would shadow the new file for every
    # reader and be checkpointed over its pages. Nothing holds it, because the
    # caller holds the lock and the follower opens no connection until after.
    for suffix in ("-wal", "-shm"):
        Path(str(out) + suffix).unlink(missing_ok=True)
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


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _is_file_doc(doc: dict) -> bool:
    return doc.get("type") in ("plain", "newnote") and "children" in doc


def _has_row(conn: sqlite3.Connection, doc_id: str) -> bool:
    return conn.execute("SELECT 1 FROM notes WHERE id = ?", (doc_id,)).fetchone() is not None


def plan_note(conn: sqlite3.Connection, doc_id: str, doc: dict | None) -> str:
    """Decide what one note id needs: 'put', 'remove' or 'skip'.

    `doc` is the note's current doc, or None if CouchDB no longer has it. Reads
    the index only; the caller applies the plan in its own transaction, and as
    the only writer nothing can change the rows in between.
    """
    if doc is None or doc.get("deleted") or doc.get("_deleted"):
        return "remove" if _has_row(conn, doc_id) else "skip"
    if not _is_file_doc(doc):
        # Not a note. Only an id that was once indexed as one has a row to drop.
        return "remove" if _has_row(conn, doc_id) else "skip"
    if ext_of(doc_path(doc)) not in indexed_exts():
        # Covers a rename to an excluded type as well as one never indexed.
        return "remove" if _has_row(conn, doc_id) else "skip"
    row = conn.execute(
        "SELECT rev, children_key, status FROM notes WHERE id = ?", (doc_id,)
    ).fetchone()
    # A failed row with an unchanged rev is put again: cheap, and it may now
    # succeed because its missing chunks have arrived.
    if row is not None and row[2] == "ok" and row[:2] == (doc.get("_rev", ""), children_key(doc)):
        return "skip"
    return "put"


def due_retries(
    conn: sqlite3.Connection, now: int, arrived: set[str], exclude: set[str]
) -> list[str]:
    """Failed note ids to re-read this cycle.

    A missing chunk arrives as an `h:` change row. Nearly every edit batch
    carries `h:` ids, so retrying every failed note on any such batch would
    undo the hourly backoff for permanently damaged notes; matching the arrived
    ids against each note's recorded missing ids targets exactly the notes that
    could now succeed.

    Those notes are always returned: an arrival is only seen in its own batch,
    so capping it out would leave the note failed until its hourly retry. They
    are bounded by the batch's chunk rows. The rest are capped at
    RETRY_MAX_PER_CYCLE, never-tried first, then oldest try.
    """
    arrivals, due = [], []
    for doc_id, first, last, missing in conn.execute(
        "SELECT n.id, r.first_failed_at, r.last_tried_at, r.missing FROM notes n"
        " LEFT JOIN retry r ON r.id = n.id WHERE n.status = 'failed'"
        " ORDER BY r.last_tried_at IS NOT NULL, r.last_tried_at, n.id"
    ):
        if doc_id in exclude:
            continue
        if first is not None and not arrived.isdisjoint(json.loads(missing)):
            arrivals.append(doc_id)
        elif len(due) < RETRY_MAX_PER_CYCLE and (
            first is None
            or now - first < RETRY_FAST_WINDOW
            or now - last >= RETRY_SLOW_INTERVAL
        ):
            due.append(doc_id)
    return arrivals + due


async def fetch_changes(
    client: ObsidianVaultClient, since: str
) -> tuple[list[dict], str, int]:
    """One longpoll `_changes` request. Returns (rows, last_seq, pending).

    `pending` is CouchDB's count of changes after this batch. It is what tells
    a reader the index is behind, which the heartbeat cannot: a follower
    catching up writes a fresh heartbeat on every batch.
    """
    http = await client._get_client()
    resp = await http.get(
        "/_changes",
        params={
            "since": since,
            "feed": "longpoll",
            "timeout": CHANGES_LONGPOLL_MS,
            "include_docs": "true",
            "limit": CHANGES_LIMIT,
        },
        timeout=CHANGES_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("results"), list)
        or "last_seq" not in data
        or not isinstance(data.get("pending"), int)
    ):
        raise RuntimeError(
            f"_changes response has no results list, last_seq or pending: {str(data)[:200]}"
        )
    for row in data["results"]:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise RuntimeError(f"_changes row without an id: {str(row)[:200]}")
    return data["results"], str(data["last_seq"]), data["pending"]


async def fetch_current_docs(
    client: ObsidianVaultClient, ids: list[str]
) -> dict[str, dict | None]:
    """Current doc per id via keyed `_all_docs`; None if CouchDB no longer has it.

    None covers both a never-existing id and a real CouchDB delete. A row that
    is neither a doc nor one of those is raised, never read as a deletion.
    """
    http = await client._get_client()
    out: dict[str, dict | None] = {}
    for i in range(0, len(ids), RETRY_READ_BATCH):
        resp = await http.post(
            "/_all_docs",
            json={"keys": ids[i : i + RETRY_READ_BATCH]},
            params={"include_docs": "true"},
        )
        resp.raise_for_status()
        for row in resp.json().get("rows", []):
            if isinstance(row.get("doc"), dict):
                out[row["key"]] = row["doc"]
            elif row.get("error") == "not_found" or (row.get("value") or {}).get("deleted"):
                out[row["key"]] = None
            else:
                raise RuntimeError(f"unexpected _all_docs row for a retried note: {str(row)[:200]}")
    missing = [d for d in ids if d not in out]
    if missing:
        raise RuntimeError(f"_all_docs returned no row for {len(missing)} retried note(s)")
    return out


def apply_cycle(
    conn: sqlite3.Connection,
    plans: list[tuple[str, str, dict | None]],
    chunks: dict[str, str],
    last_seq: str,
    now: int,
    pending: int = 0,
) -> dict[str, int]:
    """Apply one cycle's plans and advance last_seq in ONE transaction.

    Deliberately synchronous: follow() cancels an in-flight cycle on shutdown,
    and cancellation can only land at an await, so it can never split this
    transaction. Rows and the sequence they cover commit together or not at all.
    """
    counts = {"put_ok": 0, "put_failed": 0, "removed": 0, "skipped": 0}
    with conn:
        for doc_id, action, doc in plans:
            if action == "skip":
                counts["skipped"] += 1
            elif action == "remove":
                remove_note(conn, doc_id)
                conn.execute("DELETE FROM retry WHERE id = ?", (doc_id,))
                counts["removed"] += 1
            elif put_note(conn, doc, chunks) == "ok":
                conn.execute("DELETE FROM retry WHERE id = ?", (doc_id,))
                counts["put_ok"] += 1
            else:
                missing = [c for c in (chunk_ids(doc) or []) if c not in chunks]
                conn.execute(
                    "INSERT INTO retry(id, first_failed_at, last_tried_at, missing)"
                    " VALUES(?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET"
                    " last_tried_at = excluded.last_tried_at, missing = excluded.missing",
                    (doc_id, now, now, json.dumps(missing)),
                )
                counts["put_failed"] += 1
        by_status = dict(conn.execute("SELECT status, COUNT(*) FROM notes GROUP BY status"))
        set_meta(
            conn,
            last_seq=last_seq,
            pending=pending,
            heartbeat_at=now,
            note_count=by_status.get("ok", 0),
            failed_count=by_status.get("failed", 0),
        )
    return counts


async def sync_once(client: ObsidianVaultClient, conn: sqlite3.Connection, *, clock=time.time) -> dict:
    """One cycle: read a batch of changes and due retries, then commit once.

    Every CouchDB read happens before the transaction, so a transport error
    anywhere in the cycle leaves the index and last_seq exactly as they were.
    """
    since = get_meta(conn)["last_seq"]
    rows, last_seq, pending = await fetch_changes(client, since)
    now = int(clock())

    latest: dict[str, dict] = {}
    arrived: set[str] = set()
    for row in rows:
        doc_id = row["id"]
        if doc_id.startswith("h:"):
            if not row.get("deleted"):
                arrived.add(doc_id)
        elif not doc_id.startswith(NOT_NOTE_PREFIXES):
            latest[doc_id] = row  # last row per id wins
    plans: list[tuple[str, str, dict | None]] = []
    for doc_id, row in latest.items():
        doc = None if row.get("deleted") else row.get("doc")
        if doc is None and not row.get("deleted"):
            raise RuntimeError(f"_changes row for {doc_id!r} carries no doc (include_docs ignored?)")
        plans.append((doc_id, plan_note(conn, doc_id, doc), doc))

    retry_ids = due_retries(conn, now, arrived, set(latest))
    if retry_ids:
        current = await fetch_current_docs(client, retry_ids)
        plans += [(d, plan_note(conn, d, current[d]), current[d]) for d in retry_ids]

    wanted = [c for _, action, doc in plans if action == "put" for c in (chunk_ids(doc) or [])]
    chunks = await client._fetch_chunks_batched(list(dict.fromkeys(wanted))) if wanted else {}
    counts = apply_cycle(conn, plans, chunks, last_seq, now, pending)
    return {**counts, "retried": len(retry_ids), "changes": len(rows), "last_seq": last_seq,
            "pending": pending}


def index_is_current(path: Path, db_name: str, origin: str | None = None) -> bool:
    """True if `path` is an index of this schema, database, server and type list.

    A changed OBSIDIAN_SEARCH_INDEX_EXTS rebuilds: otherwise the new list would
    take effect only as notes happened to change.
    """
    if not path.exists():
        return False
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            meta = get_meta(conn)
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False
    return (
        meta.get("schema_version") == SCHEMA_VERSION
        and meta.get("fold_version") == FOLD_VERSION
        and meta.get("db_name") == db_name
        and (origin is None or meta.get("couch_origin") == origin)
        and meta.get("exts") == ",".join(sorted(indexed_exts()))
        and "last_seq" in meta
    )


async def open_index(client: ObsidianVaultClient, path: Path) -> sqlite3.Connection:
    """Open the index for following, building it first only if it is unusable."""
    if index_is_current(path, client.config.db_name, couch_origin(client.config.couch_url)):
        conn = connect(path)
        create_schema(conn)  # adds the retry table to an index built before it existed
        _log(f"follow: resumed {path} from seq {get_meta(conn)['last_seq'][:16]}")
        return conn
    result = await build(client, path)
    _log(f"follow: built {path}: {result}")
    conn = connect(path)
    create_schema(conn)
    return conn


_STOPPED = object()


async def _unless_stopped(coro, stop: asyncio.Event):
    """Run `coro`, cancelling it if `stop` is set first. Returns _STOPPED then."""
    task = asyncio.ensure_future(coro)
    waiter = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    return _STOPPED if task.cancelled() else task.result()


async def follow(
    client: ObsidianVaultClient, path: Path, *, stop_event: asyncio.Event | None = None
) -> None:
    """Keep `path` current from the change feed until `stop_event` is set.

    A CouchDB transport error (connection failure, timeout) is logged and
    retried with exponential backoff; the cycle that hit it wrote nothing, so
    no note is ever marked deleted or failed because CouchDB was unreachable.
    Any other exception propagates: systemd restarts the process, and the
    restart resumes from the last committed sequence. Holds `index_lock` for
    its whole run.
    """
    stop = stop_event or asyncio.Event()
    with index_lock(path):
        await _follow_locked(client, path, stop)


async def _follow_locked(client: ObsidianVaultClient, path: Path, stop: asyncio.Event) -> None:
    conn: sqlite3.Connection | None = None
    backoff = 1.0
    try:
        while not stop.is_set():
            try:
                if conn is None:
                    conn = await _unless_stopped(open_index(client, path), stop)
                    if conn is _STOPPED:
                        conn = None
                        return
                counts = await _unless_stopped(sync_once(client, conn), stop)
            except httpx.TransportError as exc:
                _log(f"follow: couchdb unreachable ({type(exc).__name__}: {exc}); retrying in {backoff:g}s")
                await _unless_stopped(asyncio.sleep(backoff), stop)
                backoff = min(backoff * 2, BACKOFF_CAP)
                continue
            if counts is _STOPPED:
                return
            backoff = 1.0
            if counts["put_ok"] + counts["put_failed"] + counts["removed"]:
                _log(
                    f"follow: seq {counts['last_seq'][:16]} put ok={counts['put_ok']}"
                    f" failed={counts['put_failed']} removed={counts['removed']}"
                    f" skipped={counts['skipped']} retried={counts['retried']}"
                )
    finally:
        if conn is not None:
            conn.close()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="python -m obsidian_self_mcp.search_index")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "stats", "follow"):
        p = sub.add_parser(name)
        p.add_argument("--out", type=Path, help="index file (default: per-database state path)")
    args = ap.parse_args(argv)

    config = Config()
    out = args.out or default_index_path(config.db_name)
    if args.cmd == "stats":
        for k, v in stats(out).items():
            print(f"{k}: {v}")
        return

    if args.cmd == "follow":
        async def run_follow() -> None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stop.set)
            client = ObsidianVaultClient(config)
            try:
                await follow(client, out, stop_event=stop)
            finally:
                await client.close()

        try:
            asyncio.run(run_follow())
        except IndexBusy as exc:
            _log(f"follow: {exc}")
            sys.exit(1)
        _log("follow: stopped")
        return

    async def run() -> dict:
        client = ObsidianVaultClient(config)
        try:
            with index_lock(out):
                return await build(client, out)
        finally:
            await client.close()

    try:
        result = asyncio.run(run())
    except IndexBusy as exc:
        _log(f"build refused: {exc}")
        sys.exit(1)
    print(f"built {out}: {result}")


if __name__ == "__main__":
    sys.exit(main())
