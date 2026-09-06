"""Async CouchDB client for Obsidian vault operations."""

import asyncio
import base64
import json
import logging
import os
import time
import urllib.parse
from collections import defaultdict

import httpx

from .config import Config
from .models import BacklinkInfo, FolderInfo, NoteContent, NoteMetadata, SearchResult
from .utils import (
    encode_doc_id,
    extract_frontmatter,
    extract_tags,
    extract_wikilinks,
    generate_chunk_id,
    normalize_doc_id,
    set_frontmatter,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000  # ~10KB chunks for binary
BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf",
    ".mp3", ".mp4", ".wav", ".zip", ".tar", ".gz",
}

# node_writer/ is the real livesync-commonlib-backed writer (via a vendored
# fanselau/obsidian-vault-cli submodule) that write_note()/append_note() delegate
# plain-text writes to, instead of this module's own generate_chunk_id()+raw-PUT
# path. See ISA 20260728-180000_travel-resilient-vault-sync-architecture,
# ISC-46/47/49 — the delegation exists because chunk IDs must be real
# xxhash64-based (content-addressed) to avoid unbounded chunk-doc bloat, and
# because raw PUTs bypass LiveSync's replication-safe write path entirely.
#
# Writes go to a long-lived warm daemon (node_writer/src/daemon.ts) over a
# Unix socket, not a per-write subprocess spawn — ISC-54 costed this out and
# found it eliminates the exit-13 CPU-starvation failure mode entirely under
# the same concurrency test that reliably reproduced it on the subprocess
# path (2026-07-30, 16/16 real writes across two 8-way bursts, zero
# failures). The daemon is multi-DB: credentials and db name travel per
# request, same as before, so one daemon instance serves dev and production
# without being hardcoded to either.
_WRITE_DAEMON_SOCK = os.environ.get(
    "OBSIDIAN_WRITE_DAEMON_SOCK",
    os.path.join(os.environ.get("XDG_RUNTIME_DIR", "/tmp"), "obsidian-write-daemon.sock"),
)
_WRITE_DAEMON_TIMEOUT_SECONDS = float(os.environ.get("OBSIDIAN_WRITE_TIMEOUT_SECONDS", "30"))
_WRITE_DAEMON_RETRY_DELAY_SECONDS = float(os.environ.get("OBSIDIAN_WRITE_DAEMON_RETRY_DELAY", "3"))


class NodeWriterError(RuntimeError):
    """Raised when the delegated warm-daemon writer fails, is unreachable, or times out.

    Caller-visible by design (ISC-46): distinguishes "daemon down" (ENOENT/
    ECONNREFUSED, retried once per Q3's health/liveness answer, matching
    systemd's RestartSec window) from a write-level failure or timeout the
    daemon itself reports. The daemon's response is an explicit {ok: bool}
    JSON value, not a stdout-marker heuristic — the old subprocess model's
    exit-0-masking/indeterminate-outcome handling (2026-07-29 Decisions) no
    longer applies; a socket response is unambiguous.
    """


def _replace_wikilink_target(content: str, old_name: str, new_name: str) -> str:
    """Replace [[OldName...]] wikilinks with [[NewName...]], case-insensitive on filename.

    Handles [[Name]], [[Name|alias]], [[Name#heading]], [[Name#heading|alias]],
    and full-path variants [[Folder/Name]]. Preserves heading and alias text unchanged.
    Known limitation: wikilinks inside fenced code blocks are also replaced.
    """
    import re
    old_lower = old_name.lower()

    def replacer(m: re.Match) -> str:
        full = m.group(0)
        target = m.group(1)     # path portion (no # or |)
        rest = m.group(2) or ""  # heading + alias (e.g. "#Section|display")
        filename = target.rsplit("/", 1)[-1]
        if filename.lower() != old_lower:
            return full
        prefix = target[: len(target) - len(filename)]
        return f"[[{prefix}{new_name}{rest}]]"

    pattern = re.compile(r"\[\[([^\]|#]+)([^\]]*)\]\]")
    return pattern.sub(replacer, content)


class ObsidianVaultClient:
    """Async client for reading/writing Obsidian vault docs in CouchDB."""

    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self._client: httpx.AsyncClient | None = None
        self._last_ntfy_time: float = 0.0
        self._write_locks: dict[str, asyncio.Lock] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.config.db_url,
                auth=(self.config.couch_user, self.config.couch_pass),
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Low-level helpers ──────────────────────────────────────────

    async def _get_doc(self, path: str, *, include_deleted: bool = False) -> dict | None:
        """Fetch a doc by vault path, trying both ID conventions.

        By default, filters out LiveSync-tombstoned documents (`deleted: true`
        on the doc's current revision — an app-level soft-delete flag LiveSync
        sets when a note is deleted client-side, distinct from a real CouchDB
        delete: the doc still returns 200 with its content chunks attached).
        See SAI-OQ-065. Pass include_deleted=True for the rare case a caller
        explicitly wants a tombstone (e.g. auditing what was deleted).
        """
        client = await self._get_client()
        doc_id = normalize_doc_id(path)

        # Try normalized ID first (handles '_' prefix → '/_' automatically)
        resp = await client.get(f"/{encode_doc_id(doc_id)}")
        if resp.status_code == 200:
            doc = resp.json()
            if include_deleted or not doc.get("deleted"):
                return doc

        # Try alternate convention (with/without leading slash)
        alt_id = "/" + doc_id if not doc_id.startswith("/") else doc_id[1:]
        resp = await client.get(f"/{encode_doc_id(alt_id)}")
        if resp.status_code == 200:
            doc = resp.json()
            if include_deleted or not doc.get("deleted"):
                return doc

        # Fallback: search by path field (for hash-ID format f:... used by newer LiveSync)
        path_lower = path.lower()
        all_docs = await self._get_all_file_docs(include_deleted=include_deleted)
        for doc in all_docs:
            if doc.get("path", "").lower() == path_lower:
                return doc

        return None

    async def _canonicalize_path(self, vault_path: str) -> str:
        """Resolve canonical folder casing from existing vault docs.

        Walks ancestor folders from deepest to shallowest, querying CouchDB
        for any existing doc. When found, extracts canonical folder casing from
        that doc's path field and reconstructs the full path. Filename component
        is always taken from vault_path unchanged. Falls back to vault_path if
        no ancestor folder has existing docs or if the lookup fails.
        """
        if "/" not in vault_path:
            return vault_path

        parts = vault_path.split("/")

        for depth in range(len(parts) - 1, 0, -1):
            parent_id = "/".join(p.lower() for p in parts[:depth])
            try:
                client = await self._get_client()
                resp = await client.get(
                    "/_all_docs",
                    params={
                        "startkey": '"' + parent_id + '/"',
                        "endkey": '"' + parent_id + '/￰"',
                        "limit": "1",
                        "include_docs": "true",
                    },
                )
                resp.raise_for_status()
                rows = resp.json().get("rows", [])
                if rows:
                    existing_path = rows[0]["doc"].get("path", "")
                    existing_parts = existing_path.split("/")
                    if len(existing_parts) > depth:
                        canonical_folder = "/".join(existing_parts[:depth])
                        remaining = "/".join(parts[depth:])
                        canonical = canonical_folder + "/" + remaining
                        if canonical != vault_path:
                            logger.warning(
                                "Path canonicalized: %r -> %r", vault_path, canonical
                            )
                        return canonical
            except Exception as e:
                logger.error(
                    "_canonicalize_path lookup failed for %r: %s", vault_path, e
                )
                return vault_path

        return vault_path

    async def _fetch_chunks(self, chunk_ids: list[str]) -> dict[str, str]:
        """Batch-fetch chunks via POST _all_docs. Returns {chunk_id: data}."""
        if not chunk_ids:
            return {}
        client = await self._get_client()
        resp = await client.post(
            "/_all_docs",
            json={"keys": chunk_ids},
            params={"include_docs": "true"},
        )
        resp.raise_for_status()
        result = {}
        for row in resp.json().get("rows", []):
            doc = row.get("doc")
            if doc and "data" in doc:
                result[row["id"]] = doc["data"]
        return result

    async def _get_all_file_docs(self, *, include_deleted: bool = False) -> list[dict]:
        """Fetch all file docs (skip chunks, design docs, index docs).

        By default, filters out LiveSync-tombstoned documents (`deleted: true`
        on the doc's current revision) — see SAI-OQ-065. These are live
        CouchDB docs (content chunks still attached, standard `type`/
        `children` shape), not `_all_docs`-level deletions, so they passed
        every other check here and were previously indistinguishable from
        real live notes to every method built on this one.
        """
        client = await self._get_client()
        docs = []

        # Range 1: docs before "h:" (chunk prefix)
        resp = await client.get(
            "/_all_docs",
            params={
                "include_docs": "true",
                "endkey": '"h:"',
                "inclusive_end": "false",
            },
        )
        resp.raise_for_status()
        for row in resp.json().get("rows", []):
            doc = row.get("doc", {})
            if doc.get("type") in ("plain", "newnote") and "children" in doc:
                if include_deleted or not doc.get("deleted"):
                    docs.append(doc)

        # Range 2: docs after "h:~" (after all chunks)
        resp = await client.get(
            "/_all_docs",
            params={
                "include_docs": "true",
                "startkey": '"h:~"',
            },
        )
        resp.raise_for_status()
        for row in resp.json().get("rows", []):
            doc = row.get("doc", {})
            if doc.get("type") in ("plain", "newnote") and "children" in doc:
                if include_deleted or not doc.get("deleted"):
                    docs.append(doc)

        return docs

    # ── Read operations ────────────────────────────────────────────

    async def _get_matching_docs(
        self, folder: str | None = None, *, include_deleted: bool = False
    ) -> list[dict]:
        """The complete, unsliced set of file docs matching an optional folder
        filter. _get_all_file_docs() already fetches everything from CouchDB
        with no server-side limit — the only truncation in this codebase was
        list_notes() silently slicing this already-complete list down to
        `limit` with no signal to the caller. Shared here so list_notes() and
        count_notes() can never disagree about what "all" means.
        """
        all_docs = await self._get_all_file_docs(include_deleted=include_deleted)
        if folder:
            folder_lower = folder.strip("/").lower() + "/"
            all_docs = [
                d for d in all_docs
                if d.get("path", d.get("_id", "")).lower().startswith(folder_lower)
            ]
        all_docs.sort(key=lambda d: d.get("mtime", 0), reverse=True)
        return all_docs

    async def count_notes(
        self, folder: str | None = None, *, include_deleted: bool = False
    ) -> int:
        """The real, complete, non-paginated count of notes matching an
        optional folder filter — the safe replacement for "does N look like
        the true total" guesswork against list_notes()'s (or the CLI's/MCP
        tool's) sliced output. Found live, 2026-07-30: an audit trusted
        `obsidian list`'s bare output as complete, silently missing 22 real
        files hidden past the default limit=50 slice, before being caught by
        an independent CouchDB _all_docs cross-check. This function IS that
        cross-check, built in rather than something the caller has to
        remember to do by hand.
        """
        docs = await self._get_matching_docs(folder, include_deleted=include_deleted)
        return len(docs)

    async def list_notes(
        self,
        folder: str | None = None,
        limit: int = 50,
        skip: int = 0,
        *,
        include_deleted: bool = False,
    ) -> list[NoteMetadata]:
        """List notes, optionally filtered by folder prefix.

        `limit`/`skip` are for genuine pagination (a human browsing a large
        folder) — they are NOT safe to treat as "if I get back fewer than
        limit, that's everything." Callers that need to know the true total
        (existence checks, audits, "does X still have a file" questions)
        must call count_notes() as well, or use list_notes_all(), never infer
        completeness from len(results) < limit or from the absence of any
        truncation warning in this function's own return value.
        """
        all_docs = await self._get_matching_docs(folder, include_deleted=include_deleted)

        results = []
        for doc in all_docs[skip : skip + limit]:
            results.append(NoteMetadata(
                path=doc.get("path", doc["_id"]),
                size=doc.get("size", 0),
                ctime=doc.get("ctime", 0),
                mtime=doc.get("mtime", 0),
                doc_type=doc.get("type", "plain"),
                chunk_count=len(doc.get("children", [])),
            ))
        return results

    async def list_notes_all(
        self, folder: str | None = None, *, include_deleted: bool = False
    ) -> list[NoteMetadata]:
        """list_notes() with no slicing at all — the complete, accurate
        result set. _get_matching_docs() already has everything in memory
        before any limit is applied, so this costs nothing extra over
        list_notes(); it just never throws part of the answer away. Prefer
        this (or count_notes()) over list_notes() for anything where an
        incomplete answer would be wrong, not just less convenient.
        """
        all_docs = await self._get_matching_docs(folder, include_deleted=include_deleted)
        return [
            NoteMetadata(
                path=doc.get("path", doc["_id"]),
                size=doc.get("size", 0),
                ctime=doc.get("ctime", 0),
                mtime=doc.get("mtime", 0),
                doc_type=doc.get("type", "plain"),
                chunk_count=len(doc.get("children", [])),
            )
            for doc in all_docs
        ]

    async def read_note(
        self, path: str, strict: bool = False, *, include_deleted: bool = False
    ) -> NoteContent | None:
        """Read a note's full content by reassembling chunks in order.

        `strict=True` raises ValueError if any chunk_id in `children` has no
        corresponding fetched chunk (missing, tombstoned, or replication-
        lagged), instead of silently reassembling a gap as "" (Forge review
        S2). Default stays non-strict for existing callers (e.g. the
        `read_note` MCP tool) that expect a best-effort read; callers that
        write the reassembled content back (`append_note`) must pass
        strict=True, since a silent gap there becomes permanent data loss
        rather than just a bad read.

        `include_deleted` (SAI-OQ-065) defaults to False — a LiveSync
        tombstone reads as "not found," same as a genuinely absent note.
        """
        doc = await self._get_doc(path, include_deleted=include_deleted)
        if not doc:
            return None

        chunk_ids = doc.get("children", [])
        chunks = await self._fetch_chunks(chunk_ids)

        if strict:
            missing = [cid for cid in chunk_ids if cid not in chunks]
            if missing:
                raise ValueError(
                    f"read_note(strict=True) for {path!r}: {len(missing)} of {len(chunk_ids)} "
                    f"chunks missing/unfetchable ({missing[:3]}{'...' if len(missing) > 3 else ''}) "
                    f"— refusing to reassemble a gap that a write-back would make permanent"
                )

        # Reassemble in order
        content_parts = [chunks.get(cid, "") for cid in chunk_ids]
        content = "".join(content_parts)

        is_binary = doc.get("type") == "newnote"

        return NoteContent(
            path=doc.get("path", path),
            content=content,
            size=doc.get("size", 0),
            is_binary=is_binary,
        )

    async def list_folders(self) -> list[FolderInfo]:
        """Extract unique folder paths from all file docs."""
        all_docs = await self._get_all_file_docs()
        folder_counts: dict[str, int] = defaultdict(int)

        for doc in all_docs:
            path = doc.get("path", doc.get("_id", ""))
            parts = path.rsplit("/", 1)
            if len(parts) == 2:
                folder = parts[0]
                folder_counts[folder] += 1
            else:
                folder_counts["(root)"] += 1

        results = [
            FolderInfo(path=f, note_count=c)
            for f, c in sorted(folder_counts.items())
        ]
        return results

    async def _notify_livesync(self) -> None:
        """POST to ntfy to wake mobile LiveSync clients. Rate-limited per batch_seconds."""
        cfg = self.config
        if not cfg.ntfy_url or not cfg.ntfy_topic:
            return
        now = time.time()
        if now - self._last_ntfy_time < cfg.ntfy_batch_seconds:
            return
        self._last_ntfy_time = now
        try:
            async with httpx.AsyncClient(timeout=5.0) as ntfy:
                await ntfy.post(
                    f"{cfg.ntfy_url}/{cfg.ntfy_topic}",
                    content=b"livesync",
                    headers={"Title": "Obsidian Sync", "Priority": "low", "Tags": "notebook"},
                )
        except Exception:
            logger.warning("_notify_livesync failed (non-fatal, sync-wake only)", exc_info=True)

    # ── Write operations ───────────────────────────────────────────

    def _get_write_lock(self, vault_path: str) -> asyncio.Lock:
        """Per-canonical-path lock — prevents two concurrent delegated writes
        (or a write racing an append's read-then-write window) from
        interleaving on the same note (Forge review S7, 2026-07-29)."""
        key = vault_path.lower()
        lock = self._write_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._write_locks[key] = lock
        return lock

    async def _delegate_write(self, path: str, content: str) -> None:
        """Write plain-text content via the real livesync-commonlib writer.

        Sends {path, content} plus this call's own couch_url/user/pass/
        db_name over a Unix socket to the warm write daemon
        (node_writer/src/daemon.ts), which holds a persistent
        DirectFileManipulator connection per (couchUrl, dbName) pair and
        uses the actual xxhash64 content-addressed chunking instead of this
        module's random generate_chunk_id(). Raises NodeWriterError on any
        confirmed failure — daemon unreachable after one retry, a write-
        level error the daemon reports, or a timeout on the daemon's
        response.

        Health/liveness (Q3, ISC-54): connecting to a dead/restarting
        daemon fails immediately and cleanly (ENOENT if the socket file is
        gone — clean shutdown; ECONNREFUSED if it's stale — a crash), never
        a hang, per direct testing of both failure modes during the daemon
        costing prototype. One retry after a short delay covers systemd's
        RestartSec window; a second failure is treated as a real outage,
        not retried further, so a caller doesn't silently stall.
        """
        if not (self.config.couch_url and self.config.couch_user and self.config.couch_pass and self.config.db_name):
            raise NodeWriterError(
                f"refusing to delegate write for {path!r}: one or more of couch_url/couch_user/"
                f"couch_pass/db_name is empty — an empty COUCHDB_URL would silently diverge from "
                f"whatever this Config actually points reads at (Forge review S5, carried forward "
                f"from the subprocess design)"
            )

        request = {
            "op": "write",
            "couchUrl": self.config.couch_url,
            "couchUser": self.config.couch_user,
            "couchPass": self.config.couch_pass,
            "dbName": self.config.db_name,
            "path": path,
            "content": content,
        }
        payload = (json.dumps(request) + "\n").encode("utf-8")

        async def _one_attempt() -> dict:
            reader, writer = await asyncio.open_unix_connection(_WRITE_DAEMON_SOCK)
            try:
                writer.write(payload)
                await writer.drain()
                line = await asyncio.wait_for(reader.readline(), timeout=_WRITE_DAEMON_TIMEOUT_SECONDS)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            if not line:
                raise NodeWriterError(
                    f"write daemon closed the connection without a response for {path!r} "
                    f"(socket={_WRITE_DAEMON_SOCK}) — daemon may have crashed mid-write; "
                    f"whether the write landed is NOT known, content-addressed chunks make a "
                    f"retry largely safe, but the entry-doc revision is not."
                )
            return json.loads(line.decode("utf-8"))

        try:
            resp = await _one_attempt()
        except (FileNotFoundError, ConnectionRefusedError) as exc:
            logger.warning(
                "write daemon unreachable (%s) on first attempt for %r, retrying in %.1fs",
                type(exc).__name__, path, _WRITE_DAEMON_RETRY_DELAY_SECONDS,
            )
            await asyncio.sleep(_WRITE_DAEMON_RETRY_DELAY_SECONDS)
            try:
                resp = await _one_attempt()
            except (FileNotFoundError, ConnectionRefusedError) as exc2:
                raise NodeWriterError(
                    f"write daemon unreachable at {_WRITE_DAEMON_SOCK} writing {path!r} after 1 retry "
                    f"({type(exc2).__name__}) — daemon is down or the socket path is misconfigured; "
                    f"no write was attempted, this is a clean pre-write failure, not indeterminate."
                ) from exc2
        except asyncio.TimeoutError:
            raise NodeWriterError(
                f"write daemon did not respond within {_WRITE_DAEMON_TIMEOUT_SECONDS}s writing {path!r} "
                f"— the daemon has its own shorter internal write timeout and poisons/reconnects its "
                f"connection for this db on timeout, so a stuck write does not wedge subsequent calls, "
                f"but whether THIS write landed before the daemon gave up is NOT known; content-"
                f"addressed chunks make a retry largely safe, but the entry-doc revision is not."
            )

        if not resp.get("ok"):
            raise NodeWriterError(f"write daemon reported failure for {path!r}: {resp.get('error')}")

    async def _write_note_raw_put(
        self, path: str, content: str, is_binary: bool = False
    ) -> bool:
        """Original generate_chunk_id()+raw-PUT writer (pre-delegation).

        Kept for two callers that must NOT go through `_delegate_write`:
        binary writes (vendored CLI's `write` command has no base64/chunked
        support), and `rename_note` (deliberately excluded from delegation —
        ISC-47's scope narrowing found rename's write half unassessed
        against the new path, and Forge's S3 finding confirmed rename_note
        would otherwise inherit N-sequential-subprocess latency plus an
        unconditional old-entry-delete after a partial backlink-write
        failure — both real risks this session did not sign up to fix).
        """
        client = await self._get_client()
        vault_path = path.lstrip("/")
        doc_id = normalize_doc_id(vault_path)
        encoded_id = encode_doc_id(doc_id)

        if is_binary:
            raw = content.encode("utf-8") if isinstance(content, str) else content
            encoded_content = base64.b64encode(raw).decode("ascii")
            chunks_data = [
                encoded_content[i : i + CHUNK_SIZE]
                for i in range(0, len(encoded_content), CHUNK_SIZE)
            ]
            file_size = len(raw)
            doc_type = "newnote"
        else:
            chunks_data = [content]
            file_size = len(content.encode("utf-8"))
            doc_type = "plain"

        chunk_ids = []
        for chunk_data in chunks_data:
            chunk_id = generate_chunk_id()
            resp = await client.put(
                f"/{encode_doc_id(chunk_id)}",
                json={"_id": chunk_id, "data": chunk_data, "type": "leaf"},
            )
            resp.raise_for_status()
            chunk_ids.append(chunk_id)

        now_ms = int(time.time() * 1000)
        # include_deleted=True: this is a write-mechanics existence/rev lookup
        # ("does a doc already occupy this _id, so I know PUT-with-rev vs
        # PUT-new"), not the read-path visibility filter SAI-OQ-065 added
        # elsewhere. A tombstoned doc still occupies the _id and still has a
        # _rev that must be reused, or the PUT below 409s against it.
        existing = await self._get_doc(vault_path, include_deleted=True)

        if existing:
            # `path` is otherwise create-only: prior to this fix, an update to an
            # existing doc never touched `path`, so a doc's stored casing was frozen
            # at whatever it was first created with, forever — regardless of what
            # casing any later caller (including rename_note) supplied. Found
            # 2026-08-11 tracing why case-only renames silently never took effect.
            # Canonicalize here the same way the create branch below does: folder
            # casing resolved from existing ancestor docs, filename casing taken
            # from the caller's `path` argument unchanged.
            canonical_path = await self._canonicalize_path(vault_path)
            existing["children"] = chunk_ids
            existing["mtime"] = now_ms
            existing["size"] = file_size
            existing["type"] = doc_type
            existing["path"] = canonical_path
            # Resurrect: writing content to a path clears any LiveSync
            # tombstone on it, since the note is live again (SAI-OQ-065).
            existing["deleted"] = False
            existing_id = encode_doc_id(existing["_id"])
            resp = await client.put(f"/{existing_id}", json=existing)
            if resp.status_code == 409:
                fresh = await self._get_doc(vault_path, include_deleted=True)
                if fresh:
                    fresh["children"] = chunk_ids
                    fresh["mtime"] = now_ms
                    fresh["size"] = file_size
                    fresh["type"] = doc_type
                    fresh["path"] = canonical_path
                    fresh["deleted"] = False
                    fresh_id = encode_doc_id(fresh["_id"])
                    resp = await client.put(f"/{fresh_id}", json=fresh)
            resp.raise_for_status()
        else:
            canonical_path = await self._canonicalize_path(vault_path)
            new_doc = {
                "_id": doc_id,
                "children": chunk_ids,
                "path": canonical_path,
                "ctime": now_ms,
                "mtime": now_ms,
                "size": file_size,
                "type": doc_type,
                "eden": {},
            }
            resp = await client.put(f"/{encoded_id}", json=new_doc)
            resp.raise_for_status()

        await self._notify_livesync()
        return True

    async def _write_note_delegated_locked(self, vault_path: str, content: str) -> bool:
        """Delegated (non-binary) write, assuming the caller already holds
        `_get_write_lock(vault_path)`. Not exposed directly — call via
        `write_note()`, or from `append_note()` which holds the lock across
        its own read+write span (see there for why)."""
        canonical_path = await self._canonicalize_path(vault_path)
        await self._delegate_write(canonical_path, content)
        await self._notify_livesync()
        return True

    async def write_note(
        self, path: str, content: str, is_binary: bool = False
    ) -> bool:
        """Create or update a note. Returns True on success.

        Binary writes (is_binary=True) stay on `_write_note_raw_put` — the
        vendored node_writer's `write` command has no binary/base64
        handling, so delegation is plain-text-only for now (ISC-47 honest
        scope: this session assessed create/update writes, not
        delete_note/rename_note, and binary writes were never in the
        subprocess-delegation design's assessed scope either).
        """
        vault_path = path.lstrip("/")

        if is_binary:
            return await self._write_note_raw_put(vault_path, content, is_binary=True)

        async with self._get_write_lock(vault_path):
            return await self._write_note_delegated_locked(vault_path, content)

    async def append_note(self, path: str, content: str) -> bool:
        """Append content to an existing note. Returns True on success.

        Reads the full current content and re-writes the whole note via the
        delegated writer, rather than chunk-level tail manipulation — real
        content-addressed chunking means unchanged leading chunks get the
        same chunk IDs recomputed, so this is not the size-proportional
        re-upload it would be under naive random-ID chunking.

        Holds the write lock across the ENTIRE read-then-write span (Forge
        review S7) — calling the public `write_note()` here instead of
        `_write_note_delegated_locked()` directly would deadlock, since
        `asyncio.Lock` is not reentrant and `write_note()` acquires the same
        lock again. Without a lock spanning the read too, two concurrent
        appends can both read the pre-append content before either writes,
        silently losing one append (last-writer-wins on stale content) —
        the lock exists precisely to close that window, not just to
        serialize the final write.

        Uses `strict=True` on the read (Forge review S2): a missing/
        tombstoned/replication-lagged chunk would otherwise silently
        reassemble as an empty gap in `read_note`'s default (non-strict)
        mode, and this method writes that reassembly straight back as the
        new canonical content — a transient read hole would become
        permanent data loss instead of just a bad read.
        """
        vault_path = path.lstrip("/")
        async with self._get_write_lock(vault_path):
            note = await self.read_note(path, strict=True)
            if not note:
                raise ValueError(f"Note not found: {path}")
            if note.is_binary:
                raise ValueError(f"Cannot append to binary file: {path}")
            return await self._write_note_delegated_locked(vault_path, note.content + content)

    async def delete_note(self, path: str) -> bool:
        """Soft-delete a note the way LiveSync itself does. Returns True on success.

        Sets `deleted: true` on the entry document and bumps `mtime`, leaving
        `children`, the chunk id list, entirely untouched. This is the same write
        livesync-commonlib performs for `newnote`/`plain` documents in
        `deleteDBEntryByPath` (EntryManagerImpls.ts): chunks are never deleted, and
        even LiveSync's own `deleteMetadataOfDeletedFiles` setting only escalates to
        a CouchDB `_deleted` on the *entry* doc, never on a chunk. Because it is the
        same write a real client makes, it propagates to Obsidian by construction
        rather than by hoping a direct CouchDB delete reaches the app's view.

        SAI-OQ-074. Until 2026-09-06 this method issued an unconditional DELETE for
        every id in `children`. LiveSync chunk ids are content-addressed, so
        byte-identical content across notes resolves to ONE shared chunk document -
        deleting any note destroyed chunks that other notes still referenced,
        truncating them mid-word, returning success, logging nothing. Measured on
        production 2026-08-28: 9,176 of 98,256 chunks (9.34%) multi-referenced,
        4,369 of 13,408 notes (32.6%) unsafe to delete, worst chunk referenced by
        521 notes. Confirmed inherited from upstream, not a local regression.

        Soft delete also changes the cost of the stale-revision race that both write
        paths share. A hard DELETE on a stale rev destroys a revision nobody read -
        the retry removes whatever is current, which may be content written between
        our read and our delete. A soft delete on a stale rev merely *flags* that
        revision, leaves `children` intact, and is undone by writing to the path
        again (`_write_note_raw_put` clears the flag, SAI-OQ-065). The failure mode
        moves from silent destruction to a reversible flag.

        Read paths already filter these documents out (SAI-OQ-065), so a soft-deleted
        note is invisible to read_note/list_notes/search/count_notes exactly as a
        removed one was. There is deliberately NO purge counterpart: every
        hard-delete path in this system has produced a defect, LiveSync performs
        essentially none, and an entry-doc purge would not touch the orphaned chunk
        documents that are the only thing genuinely accumulating (VPSO-OQ-067).

        Holds the write lock for the same reason the other write paths do: without
        it a concurrent write_note and delete_note on one path interleave, and
        last-writer-wins decides whether the note exists.

        include_deleted=True on the lookup: a delete must still find an
        already-tombstoned doc, which makes re-deleting idempotent rather than a
        spurious "Note not found."
        """
        vault_path = path.lstrip("/")

        async with self._get_write_lock(vault_path):
            found = await self._soft_delete_entry_doc(vault_path, missing_ok=True)
            if not found:
                raise ValueError(f"Note not found: {path}")
            await self._notify_livesync()
            return True

    async def _soft_delete_entry_doc(
        self, vault_path: str, *, missing_ok: bool = False
    ) -> bool:
        """Flag a note's entry document deleted, LiveSync-style. Chunks untouched.

        THE ONLY PLACE IN THIS CLIENT THAT REMOVES A NOTE. Both `delete_note` and
        `rename_note` route through here so there is exactly one implementation of
        "make a note go away" and the two cannot drift apart, the drift is what
        SAI-OQ-074 was: `rename_note` was chunk-safe, `delete_note` was not, and
        the safe method sat one function away from the unsafe one for two months
        without being reached for.

        Writes what livesync-commonlib's `deleteDBEntryByPath` writes for
        `newnote`/`plain` documents (EntryManagerImpls.ts): `deleted: true`,
        `mtime` bumped, `children` left completely alone. Chunk documents are never
        deleted here, LiveSync chunk ids are content-addressed, so byte-identical
        content across notes resolves to one shared chunk, and deleting it truncates
        every other note that references it. Orphaned chunks are harmless and
        LiveSync GC handles them; a note with its body silently removed is not
        harmless and nothing handles it.

        Replaces the previous `_delete_entry_doc`, which issued a hard CouchDB
        `DELETE` on the entry document. That was chunk-safe but carried two further
        defects this does not: a direct CouchDB delete may not reach Obsidian's own
        view of the vault, and on a 409 the retry deleted whatever revision was
        current, destroying an edit written between our read and our delete, which
        during a rename is the only remaining copy of the note. A soft delete on a
        stale revision merely flags it, and is undone by writing to the path again.

        Lock-free by design. Callers own their own locking: `delete_note` holds the
        per-path write lock, `rename_note` sequences a multi-document mutation.
        `asyncio.Lock` is not reentrant, so this must not acquire one itself, the
        same split as `_write_note_delegated_locked` vs `write_note`.

        `missing_ok=True` returns False for an absent path instead of raising, which
        is what `rename_note` wants after a partial failure. Looks the doc up with
        `include_deleted=True` (SAI-OQ-065) so re-deleting an already-tombstoned
        note is idempotent rather than a spurious "Note not found".
        """
        client = await self._get_client()

        doc = await self._get_doc(vault_path, include_deleted=True)
        if not doc:
            if missing_ok:
                return False
            raise ValueError(f"Note not found: {vault_path}")

        def _tombstone(target: dict) -> dict:
            # `children` is deliberately not read, not filtered, not touched.
            target["deleted"] = True
            target["mtime"] = int(time.time() * 1000)
            return target

        resp = await client.put(f"/{encode_doc_id(doc['_id'])}", json=_tombstone(doc))
        if resp.status_code == 409:
            fresh = await self._get_doc(vault_path, include_deleted=True)
            if fresh:
                resp = await client.put(
                    f"/{encode_doc_id(fresh['_id'])}", json=_tombstone(fresh)
                )
        resp.raise_for_status()
        return True

    async def rename_note(self, old_path: str, new_path: str) -> str:
        """Rename a note and update all wikilink backlinks at the CouchDB layer.

        Calls get_backlinks before any mutation. For each source note that links to
        old_path, replaces [[OldName...]] wikilinks with [[NewName...]]. Then writes
        the new note and deletes the old one. Raises ValueError if old_path not found
        or new_path already exists. Returns a summary string with backlink counts.
        """
        old_path = old_path.lstrip("/")
        new_path = new_path.lstrip("/")

        old_doc = await self._get_doc(old_path)
        if not old_doc:
            raise ValueError(f"Source note not found: {old_path}")

        # CouchDB doc IDs are always fully lowercased (normalize_doc_id), so a
        # case-only rename (e.g. "bch_cstate.md" -> "BCH_Cstate.md") computes the
        # *same* _id for old_path and new_path — old_doc and new_doc below are the
        # same underlying document, not a genuine conflict. Found 2026-08-11: this
        # used to be indistinguishable from "destination already exists" and
        # unconditionally rejected every case-only rename.
        same_underlying_doc = normalize_doc_id(old_path) == normalize_doc_id(new_path)

        # include_deleted defaults to False here deliberately (SAI-OQ-065):
        # a LiveSync-tombstoned new_path must NOT count as "already exists" —
        # that was the exact false-positive collision this bug caused
        # (feedback_livesync_tombstone_rename_collision). The write below
        # (_write_note_raw_put) looks the doc up again with include_deleted=
        # True to correctly resurrect/overwrite it if it was a tombstone.
        new_doc = await self._get_doc(new_path)
        if new_doc and not same_underlying_doc:
            raise ValueError(f"Destination already exists: {new_path}")

        old_name = old_path.rsplit("/", 1)[-1]
        if old_name.endswith(".md"):
            old_name = old_name[:-3]
        new_name = new_path.rsplit("/", 1)[-1]
        if new_name.endswith(".md"):
            new_name = new_name[:-3]

        old_note = await self.read_note(old_path)
        if not old_note:
            raise ValueError(f"Could not read source note: {old_path}")

        backlinks = await self.get_backlinks(old_path)

        # Write new file first — if this fails, no state has been mutated.
        # Also apply wikilink replacement to the content itself (handles self-links).
        # Uses _write_note_raw_put (NOT the delegated write_note) deliberately —
        # rename_note's write half was explicitly excluded from this session's
        # ISC-47 scope, and Forge review S3 confirmed inheriting delegation here
        # would add N-sequential-subprocess latency across backlinks plus an
        # unconditional old-entry-delete after a partial backlink-write failure.
        new_content_body = _replace_wikilink_target(old_note.content, old_name, new_name)
        await self._write_note_raw_put(new_path, new_content_body)

        # Update backlink sources now that new_path exists.
        updated = 0
        warnings: list[str] = []
        for bl in backlinks:
            if bl.source_path == old_path:
                # Self-link already handled by writing updated body above.
                updated += 1
                continue
            try:
                note = await self.read_note(bl.source_path)
                if not note or note.is_binary:
                    warnings.append(f"skipped {bl.source_path} (unreadable or binary)")
                    continue
                updated_content = _replace_wikilink_target(note.content, old_name, new_name)
                if updated_content != note.content:
                    await self._write_note_raw_put(bl.source_path, updated_content)
                    updated += 1
            except Exception as exc:
                warnings.append(f"failed {bl.source_path}: {exc}")

        # Soft-delete the old entry doc only — do not delete chunks.
        # LiveSync-authored notes may share content-addressed chunk IDs across files;
        # deleting chunks risks breaking unrelated notes. Orphaned chunks are harmless.
        #
        # 2026-09-06 (SAI-OQ-074): this comment was accurate about intent and wrong
        # about mechanism for two months, the helper it called hard-DELETEd the
        # entry document rather than soft-deleting it. It now genuinely soft-deletes,
        # through the same single writer `delete_note` uses.
        #
        # Adjacent pre-existing bug fixed alongside this session's delegation work
        # (Forge review S3, 2026-07-29): this delete used to run unconditionally even
        # when some backlink updates failed above, permanently destroying the only
        # path back to the old note while leaving broken wikilinks behind, reported
        # only as a substring of the returned summary. Skip it when there were
        # warnings — old_path stays resolvable (and re-renameable) until a caller
        # explicitly confirms the backlink warnings are acceptable.
        # Case-only rename: old_path and new_path are the same _id, and the
        # _write_note_raw_put call above already updated that single document in
        # place (new path casing, new content). Deleting "old_path" here would
        # delete that same just-written document — found 2026-08-11 while fixing
        # the case-only-rename path; would have silently destroyed every case-only
        # rename immediately after correctly performing it.
        if same_underlying_doc:
            pass
        elif not warnings:
            # Capture the result rather than discarding it (Forge review,
            # 2026-09-06). `missing_ok=True` turns an unresolvable old entry into a
            # False return instead of a raise, which is what we want here, but
            # silently dropping it would report a rename as clean while the old
            # path is still live. rename_note holds no write lock (see the
            # concurrency note below), so a concurrent writer really can move this
            # doc out from under us between the existence check above and here.
            if not await self._soft_delete_entry_doc(old_path, missing_ok=True):
                warnings.append(
                    f"old entry {old_path} could not be soft-deleted, "
                    f"entry doc not found at tombstone time; old path may still resolve"
                )
            else:
                # Parity with delete_note (Forge review, 2026-09-06): the tombstone
                # on old_path is the last mutation this function makes and the one
                # that actually makes the old note disappear, but nothing else here
                # signals it. The earlier _write_note_raw_put calls each notify
                # internally, so on a fast rename this is masked by the rate-limit
                # window still being open, on a rename with many backlinks, or with
                # none, it is not.
                await self._notify_livesync()
        else:
            warnings.append(f"old entry {old_path} NOT deleted — backlink warnings present")

        result = (
            f"Renamed: {old_path} → {new_path} | "
            f"backlinks found: {len(backlinks)}, updated: {updated}"
        )
        if warnings:
            result += " | warnings: " + "; ".join(warnings)
        return result

    # ── Search ─────────────────────────────────────────────────────

    async def search_notes(
        self, query: str, folder: str | None = None, limit: int = 20
    ) -> list[SearchResult]:
        """Search note content using chunk scanning with reverse map."""
        client = await self._get_client()

        # Build chunk-to-parent reverse map
        all_docs = await self._get_all_file_docs()
        chunk_to_parent: dict[str, dict] = {}
        for doc in all_docs:
            for cid in doc.get("children", []):
                chunk_to_parent[cid] = doc

        # Search chunks using Mango query with regex
        import re
        query_escaped = re.escape(query)

        mango = {
            "selector": {
                "type": "leaf",
                "data": {"$regex": f"(?i){query_escaped}"},
            },
            "fields": ["_id", "data"],
            "limit": 5000,
        }
        resp = await client.post("/_find", json=mango)
        resp.raise_for_status()
        matching_chunks = resp.json().get("docs", [])

        # Group by parent note
        note_matches: dict[str, list[str]] = defaultdict(list)
        for chunk in matching_chunks:
            chunk_id = chunk["_id"]
            parent = chunk_to_parent.get(chunk_id)
            if not parent:
                continue
            parent_path = parent.get("path", parent.get("_id", ""))

            # Filter by folder if specified
            if folder:
                folder_lower = folder.strip("/").lower() + "/"
                if not parent_path.lower().startswith(folder_lower):
                    continue

            # Extract snippet
            data = chunk.get("data", "")
            pattern = re.compile(re.escape(query), re.IGNORECASE)
            match = pattern.search(data)
            if match:
                start = max(0, match.start() - 60)
                end = min(len(data), match.end() + 60)
                snippet = data[start:end].replace("\n", " ").strip()
                if start > 0:
                    snippet = "..." + snippet
                if end < len(data):
                    snippet = snippet + "..."
                note_matches[parent_path].append(snippet)

        # Build results sorted by match count
        results = []
        for path, snippets in note_matches.items():
            results.append(SearchResult(
                path=path,
                matches=len(snippets),
                snippets=snippets[:3],  # Cap at 3 snippets per note
            ))
        results.sort(key=lambda r: r.matches, reverse=True)
        return results[:limit]

    # ── Frontmatter operations ─────────────────────────────────────

    async def read_frontmatter(
        self, path: str, *, include_deleted: bool = False
    ) -> dict | None:
        """Read and parse frontmatter from a note. Returns None if no frontmatter
        (or if the note itself doesn't exist / is a filtered-out tombstone —
        see read_note's include_deleted)."""
        note = await self.read_note(path, include_deleted=include_deleted)
        if not note or note.is_binary:
            return None
        fm, _ = extract_frontmatter(note.content)
        return fm

    async def update_frontmatter(self, path: str, properties: dict) -> bool:
        """Merge properties into a note's frontmatter. Creates frontmatter if absent."""
        note = await self.read_note(path)
        if not note:
            raise ValueError(f"Note not found: {path}")
        if note.is_binary:
            raise ValueError(f"Cannot set frontmatter on binary file: {path}")
        new_content = set_frontmatter(note.content, properties)
        return await self.write_note(path, new_content)

    # ── Tag operations ─────────────────────────────────────────────

    async def _read_note_content(self, doc: dict) -> str | None:
        """Read content from a file doc (fetch + reassemble chunks)."""
        chunk_ids = doc.get("children", [])
        if not chunk_ids:
            return None
        chunks = await self._fetch_chunks(chunk_ids)
        return "".join(chunks.get(cid, "") for cid in chunk_ids)

    async def list_tags(self, folder: str | None = None) -> dict[str, int]:
        """Scan all notes and return tag -> count mapping."""
        all_docs = await self._get_all_file_docs()
        if folder:
            folder_lower = folder.strip("/").lower() + "/"
            all_docs = [
                d for d in all_docs
                if d.get("path", d.get("_id", "")).lower().startswith(folder_lower)
            ]

        tag_counts: dict[str, int] = defaultdict(int)
        for doc in all_docs:
            if doc.get("type") == "newnote":
                continue
            content = await self._read_note_content(doc)
            if not content:
                continue
            for tag in extract_tags(content):
                tag_counts[tag] += 1

        return dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True))

    async def search_by_tag(
        self, tag: str, folder: str | None = None, limit: int = 20
    ) -> list[NoteMetadata]:
        """Find notes containing a specific tag (frontmatter or inline)."""
        all_docs = await self._get_all_file_docs()
        if folder:
            folder_lower = folder.strip("/").lower() + "/"
            all_docs = [
                d for d in all_docs
                if d.get("path", d.get("_id", "")).lower().startswith(folder_lower)
            ]

        results = []
        tag_lower = tag.lower().lstrip("#")
        for doc in all_docs:
            if doc.get("type") == "newnote":
                continue
            content = await self._read_note_content(doc)
            if not content:
                continue
            note_tags = [t.lower() for t in extract_tags(content)]
            if tag_lower in note_tags:
                results.append(NoteMetadata(
                    path=doc.get("path", doc["_id"]),
                    size=doc.get("size", 0),
                    ctime=doc.get("ctime", 0),
                    mtime=doc.get("mtime", 0),
                    doc_type=doc.get("type", "plain"),
                    chunk_count=len(doc.get("children", [])),
                ))
                if len(results) >= limit:
                    break
        return results

    # ── Link / backlink operations ─────────────────────────────────

    async def get_outbound_links(self, path: str) -> list[str]:
        """Extract wikilink targets from a single note."""
        note = await self.read_note(path)
        if not note or note.is_binary:
            return []
        return extract_wikilinks(note.content)

    async def get_backlinks(self, path: str) -> list[BacklinkInfo]:
        """Find all notes that contain a wikilink pointing to the given path."""
        import re

        # Normalize target: strip folder prefix and extension for matching
        target_name = path.rsplit("/", 1)[-1]  # filename
        if target_name.endswith(".md"):
            target_name = target_name[:-3]
        target_lower = target_name.lower()

        all_docs = await self._get_all_file_docs()
        results = []

        for doc in all_docs:
            doc_path = doc.get("path", doc.get("_id", ""))
            if doc.get("type") == "newnote":
                continue
            content = await self._read_note_content(doc)
            if not content:
                continue

            links = extract_wikilinks(content)
            link_names_lower = [l.rsplit("/", 1)[-1].lower() for l in links]

            if target_lower in link_names_lower:
                # Extract context snippet around the link
                pattern = re.compile(
                    r"(?:^|\n)([^\n]*\[\[" + re.escape(target_name) + r"[^\]]*\]\][^\n]*)",
                    re.IGNORECASE,
                )
                m = pattern.search(content)
                ctx = m.group(1).strip() if m else ""
                results.append(BacklinkInfo(source_path=doc_path, context=ctx))

        return results
