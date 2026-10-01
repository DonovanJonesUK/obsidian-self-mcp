"""Query path for the SQLite search index (read-only).

The follower in `search_index` writes the index; this module only reads it. It
imports nothing from `client`, so `client.search_notes` can use it lazily.

Availability follows SAI-DEC-240: the index is unavailable when the file is
missing or unreadable, its schema_version, db_name or CouchDB server does not
match, its heartbeat is older than the threshold, or the follower reports more
changes still to apply than the threshold allows. The caller then announces a
fallback to the live scan on the first line of the result.
"""

from __future__ import annotations

import itertools
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from .models import SearchResult
from .search_fold import ascii_query_equivalents, needs_scan
from .search_index import SCHEMA_VERSION, default_index_path, get_meta

DEFAULT_STALE_SECONDS = 300
DEFAULT_MAX_PENDING = 1000
SNIPPET_CONTEXT = 60
SNIPPETS_PER_NOTE = 3
FAILED_LISTED = 10
# FTS5's trigram tokenizer cannot serve a query shorter than this.
TRIGRAM_MIN = 3
_OFF = {"0", "false", "off", "no"}
_ON = {"", "1", "true", "on", "yes"}


class IndexUnavailable(Exception):
    """The index cannot answer; the message is the reason shown to the caller."""


@dataclass
class IndexAnswer:
    results: list[SearchResult]
    total_matched: int
    footer: str
    failed_in_scope: list[str] = field(default_factory=list)


def index_disabled_reason() -> str | None:
    """None if the index should be used, else why not, for the fallback notice."""
    raw = os.environ.get("OBSIDIAN_SEARCH_INDEX", "1")
    value = raw.strip().lower()
    if value in _ON:
        return None
    if value in _OFF:
        return f"index disabled (OBSIDIAN_SEARCH_INDEX={raw})"
    return f"index disabled (OBSIDIAN_SEARCH_INDEX={raw!r} is not a recognised on/off value)"


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        raise IndexUnavailable(f"{name}={raw!r} is not a positive whole number")
    return value


def stale_seconds() -> int:
    return _positive_int_env("OBSIDIAN_SEARCH_INDEX_STALE_SECONDS", DEFAULT_STALE_SECONDS)


def max_pending() -> int:
    return _positive_int_env("OBSIDIAN_SEARCH_INDEX_MAX_PENDING", DEFAULT_MAX_PENDING)


def validate_query(query: str, limit: int) -> None:
    """Refuse a search no path can answer sensibly, before either path runs.

    An empty query matches at every position of every note: unbounded work for
    no information.
    """
    if not query.strip():
        raise ValueError("search query is empty")
    if limit < 1:
        raise ValueError(f"limit must be at least 1, not {limit}")


def _open_ro(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise IndexUnavailable("index file missing")
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.DatabaseError as exc:
        raise IndexUnavailable(f"index unreadable: {exc}") from exc


def check_available(
    conn: sqlite3.Connection, db_name: str, origin: str | None = None
) -> dict[str, str]:
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
    if origin is not None and meta.get("couch_origin") != origin:
        raise IndexUnavailable(
            f"index is for server {meta.get('couch_origin')!r}, not {origin!r}"
        )
    try:
        age = time.time() - int(meta["heartbeat_at"])
    except (KeyError, ValueError):
        raise IndexUnavailable("no heartbeat recorded") from None
    if age > stale_seconds():
        raise IndexUnavailable(f"heartbeat {int(age // 60)} min old")
    pending = meta.get("pending")
    if pending is not None and pending.isdigit() and int(pending) > max_pending():
        raise IndexUnavailable(f"index is {pending} changes behind CouchDB")
    return meta


def _fts_phrase(query: str) -> str:
    return '"' + query.replace('"', '""') + '"'


def _candidate_sql(query: str, folder_prefix: str | None) -> tuple[str, list]:
    """Return the candidate SELECT and its arguments for one search.

    FTS5 narrows the candidates only where it cannot miss a hit: a query of at
    least three characters, none of which folds differently in SQLite than in
    Python (`search_fold`). Notes flagged `fold_odd` are always candidates.
    A shorter ASCII query is narrowed by LIKE. Every other query reads every ok
    note in scope; measured on production 2026-10-01, the worst case, `e`, took
    1.4 s over 50 M characters.
    """
    where, params = ["n.status = 'ok'"], []
    if folder_prefix:
        where.append("substr(n.path_lc, 1, ?) = ?")
        params += [len(folder_prefix), folder_prefix]
    scope = " AND ".join(where)
    if len(query) >= TRIGRAM_MIN and not needs_scan(query):
        # The candidate rowids are computed once, then joined. CROSS JOIN pins
        # that order: with a plain JOIN the planner drove from notes through the
        # status index and re-ran the MATCH once per note, 8 to 23 s on
        # production against 0.03 s. The UNION runs on rowids, never bodies.
        sql = (
            "SELECT n.path, f.body FROM ("
            "SELECT rowid FROM notes_fts WHERE notes_fts MATCH ? "
            "UNION SELECT rowid FROM notes WHERE fold_odd = 1"
            ") c CROSS JOIN notes n ON n.rowid = c.rowid "
            f"CROSS JOIN notes_fts f ON f.rowid = c.rowid WHERE {scope}"
        )
        args = [_fts_phrase(query), *params]
    elif query.isascii() and not needs_scan(query):
        # SQLite's LIKE folds ASCII case only, so notes holding a non-ASCII
        # character Python treats as one of the query's letters (the Kelvin
        # sign for k, for example) are taken too. Python confirms every hit.
        like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        cond, extra = "f.body LIKE ? ESCAPE '\\'", [like]
        equivalents = ascii_query_equivalents(query)
        if equivalents:
            cond, extra = f"({cond} OR f.body GLOB ?)", [like, f"*[{equivalents}]*"]
        sql = (
            "SELECT n.path, f.body FROM notes n CROSS JOIN notes_fts f ON f.rowid = n.rowid "
            f"WHERE {scope} AND {cond}"
        )
        args = [*params, *extra]
    else:
        sql = (
            "SELECT n.path, f.body FROM notes n CROSS JOIN notes_fts f ON f.rowid = n.rowid "
            f"WHERE {scope}"
        )
        args = params
    return sql, args


def _candidates(conn: sqlite3.Connection, query: str, folder_prefix: str | None):
    """Yield (path, body) for ok notes that may contain the query."""
    sql, args = _candidate_sql(query, folder_prefix)
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
    db_name: str,
    query: str,
    folder: str | None,
    limit: int,
    path: Path | None = None,
    origin: str | None = None,
) -> IndexAnswer:
    """Answer one search from the index, or raise IndexUnavailable.

    A candidate is only reported after a case-insensitive substring check in
    Python, so the FTS5 trigram match can over-select but never decides a hit.
    Ranking is by occurrence count, then path. Occurrences are counted with
    `findall`, never by materialising match objects: a one-letter query has
    millions of them.
    """
    validate_query(query, limit)
    conn = _open_ro(path or default_index_path(db_name))
    try:
        meta = check_available(conn, db_name, origin)
        folder_prefix = folder.strip("/").lower() + "/" if folder else None
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        hits: list[tuple[int, str, list[str]]] = []
        for note_path, body in _candidates(conn, query, folder_prefix):
            count = len(pattern.findall(body))
            if not count:
                continue
            snippets = [
                _snippet(body, m)
                for m in itertools.islice(pattern.finditer(body), SNIPPETS_PER_NOTE)
            ]
            hits.append((count, note_path, snippets))
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

    results = [SearchResult(path=p, matches=n, snippets=s) for n, p, s in hits[:limit]]
    footer = (
        f"index: {meta.get('note_count', '?')} notes, types {meta.get('exts', '?')}; "
        f"{len(hits)} matched, showing {len(results)}; follower heartbeat {age}s ago, "
        f"{meta.get('pending', '?')} changes pending"
    )
    return IndexAnswer(results, len(hits), footer, failed)


def format_failed(failed: list[str]) -> str:
    if not failed:
        return ""
    shown = ", ".join(failed[:FAILED_LISTED])
    more = f" and {len(failed) - FAILED_LISTED} more" if len(failed) > FAILED_LISTED else ""
    return (
        f"{len(failed)} note(s) in scope could not be indexed, so were not "
        f"searched: {shown}{more}"
    )
