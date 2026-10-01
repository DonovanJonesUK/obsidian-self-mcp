"""Query path for the SQLite search index (read-only).

The follower in `search_index` writes the index; this module only reads it. It
imports nothing from `client`, so `client.search_notes` can use it lazily.

Availability follows SAI-DEC-240: the index is unavailable when the file is
missing or unreadable, its schema_version or db_name does not match, or its
heartbeat is older than the threshold. The caller then announces a fallback to
the live scan on the first line of the result.
"""

from __future__ import annotations

import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from .models import SearchResult
from .search_index import SCHEMA_VERSION, default_index_path, get_meta

DEFAULT_STALE_SECONDS = 300
SNIPPET_CONTEXT = 60
SNIPPETS_PER_NOTE = 3
FAILED_LISTED = 10


class IndexUnavailable(Exception):
    """The index cannot answer; the message is the reason shown to the caller."""


@dataclass
class IndexAnswer:
    results: list[SearchResult]
    total_matched: int
    footer: str
    failed_in_scope: list[str] = field(default_factory=list)


def index_enabled() -> bool:
    return os.environ.get("OBSIDIAN_SEARCH_INDEX", "1").strip() != "0"


def stale_seconds() -> int:
    raw = os.environ.get("OBSIDIAN_SEARCH_INDEX_STALE_SECONDS")
    try:
        return int(raw) if raw else DEFAULT_STALE_SECONDS
    except ValueError:
        return DEFAULT_STALE_SECONDS


def _open_ro(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise IndexUnavailable("index file missing")
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.DatabaseError as exc:
        raise IndexUnavailable(f"index unreadable: {exc}") from exc


def check_available(conn: sqlite3.Connection, db_name: str) -> dict[str, str]:
    """Return the meta table if the index may be queried, else raise."""
    try:
        meta = get_meta(conn)
    except sqlite3.DatabaseError as exc:
        raise IndexUnavailable(f"index unreadable: {exc}") from exc
    if meta.get("schema_version") != SCHEMA_VERSION:
        raise IndexUnavailable(
            f"schema_version {meta.get('schema_version')!r} is not {SCHEMA_VERSION!r}"
        )
    if meta.get("db_name") != db_name:
        raise IndexUnavailable(
            f"index is for database {meta.get('db_name')!r}, not {db_name!r}"
        )
    try:
        age = time.time() - int(meta["heartbeat_at"])
    except (KeyError, ValueError):
        raise IndexUnavailable("no heartbeat recorded") from None
    if age > stale_seconds():
        raise IndexUnavailable(f"heartbeat {int(age // 60)} min old")
    return meta


def _fts_phrase(query: str) -> str:
    return '"' + query.replace('"', '""') + '"'


def _candidates(conn: sqlite3.Connection, query: str, folder_prefix: str | None):
    """Yield (path, body) for ok notes that may contain the query."""
    where, params = ["n.status = 'ok'"], []
    if folder_prefix:
        where.append("substr(n.path_lc, 1, ?) = ?")
        params += [len(folder_prefix), folder_prefix]
    scope = " AND ".join(where)
    if len(query) >= 3:
        sql = (
            "SELECT n.path, f.body FROM notes_fts f JOIN notes n ON n.rowid = f.rowid "
            f"WHERE notes_fts MATCH ? AND {scope}"
        )
        args = [_fts_phrase(query), *params]
    elif query.isascii():
        like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        sql = (
            "SELECT n.path, f.body FROM notes_fts f JOIN notes n ON n.rowid = f.rowid "
            f"WHERE f.body LIKE ? ESCAPE '\\' AND {scope}"
        )
        args = [like, *params]
    else:
        sql = (
            "SELECT n.path, f.body FROM notes_fts f JOIN notes n ON n.rowid = f.rowid "
            f"WHERE {scope}"
        )
        args = params
    yield from conn.execute(sql, args)


def _snippet(body: str, match: re.Match) -> str:
    start = max(0, match.start() - SNIPPET_CONTEXT)
    end = min(len(body), match.end() + SNIPPET_CONTEXT)
    text = body[start:end].replace("\n", " ").strip()
    if start > 0:
        text = "..." + text
    if end < len(body):
        text = text + "..."
    return text


def query_index(
    db_name: str, query: str, folder: str | None, limit: int, path: Path | None = None
) -> IndexAnswer:
    """Answer one search from the index, or raise IndexUnavailable.

    A candidate is only reported after a case-insensitive substring check in
    Python, so the FTS5 trigram match can over-select but never decides a hit.
    Ranking is by occurrence count, then path.
    """
    conn = _open_ro(path or default_index_path(db_name))
    try:
        meta = check_available(conn, db_name)
        folder_prefix = folder.strip("/").lower() + "/" if folder else None
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        hits: list[tuple[int, str, list[str]]] = []
        for note_path, body in _candidates(conn, query, folder_prefix):
            matches = list(pattern.finditer(body))
            if not matches:
                continue
            snippets = [_snippet(body, m) for m in matches[:SNIPPETS_PER_NOTE]]
            hits.append((len(matches), note_path, snippets))
        hits.sort(key=lambda h: (-h[0], h[1]))

        scope_sql, scope_args = "status = 'failed'", []
        if folder_prefix:
            scope_sql += " AND substr(path_lc, 1, ?) = ?"
            scope_args = [len(folder_prefix), folder_prefix]
        failed = [
            r[0]
            for r in conn.execute(
                f"SELECT path FROM notes WHERE {scope_sql} ORDER BY path", scope_args
            )
        ]
        age = int(time.time() - int(meta["heartbeat_at"]))
    except sqlite3.DatabaseError as exc:
        raise IndexUnavailable(f"index unreadable: {exc}") from exc
    finally:
        conn.close()

    results = [
        SearchResult(path=p, matches=n, snippets=s) for n, p, s in hits[: max(limit, 1)]
    ]
    footer = (
        f"index: {meta.get('note_count', '?')} notes, types {meta.get('exts', '?')}; "
        f"{len(hits)} matched, showing {len(results)}; follower heartbeat {age}s ago"
    )
    return IndexAnswer(results, len(hits), footer, failed)


def format_failed(failed: list[str]) -> str:
    if not failed:
        return ""
    shown = ", ".join(failed[:FAILED_LISTED])
    more = f" and {len(failed) - FAILED_LISTED} more" if len(failed) > FAILED_LISTED else ""
    return (
        f"{len(failed)} note(s) in scope could not be indexed (missing chunks), so "
        f"were not searched: {shown}{more}"
    )
