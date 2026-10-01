"""Tests for the search index change follower.

The defects these guard against are silent ones: text left searchable after
its note was edited or deleted, a gap indexed as if it were the note, a
sequence advanced past rows that were never written, and an outage read as a
wave of deletions. Every case drives the real HTTP code paths through an
httpx.MockTransport standing in for CouchDB.
"""

import asyncio
import json
import sqlite3

import httpx
import pytest

from obsidian_self_mcp import search_index as si
from obsidian_self_mcp.client import ObsidianVaultClient
from obsidian_self_mcp.config import Config

DB = "fake-test-db"


class FakeCouch:
    """Docs by id plus an append-only change log, served over a MockTransport."""

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.log: list[tuple[int, str, bool]] = []  # (seq, id, couch-deleted)
        self.fail = False
        self.requests: list[str] = []
        self.on_changes = None  # called with the request's since, before answering

    def seq(self) -> int:
        return self.log[-1][0] if self.log else 0

    def put_chunk(self, cid, data):
        self.docs[cid] = {"_id": cid, "_rev": "1-c", "type": "leaf", "data": data}
        self.log.append((self.seq() + 1, cid, False))

    def put_note(self, path, children, rev="1-a", **extra):
        doc = {"_id": path.lower(), "path": path, "type": "plain",
               "children": list(children), "_rev": rev, **extra}
        self.docs[doc["_id"]] = doc
        self.log.append((self.seq() + 1, doc["_id"], False))

    def couch_delete(self, doc_id):
        del self.docs[doc_id]
        self.log.append((self.seq() + 1, doc_id, True))

    def handler(self, request: httpx.Request) -> httpx.Response:
        tail = request.url.path.split(f"/{DB}", 1)[1]
        self.requests.append(f"{request.method} {tail}")
        if self.fail:
            raise httpx.ConnectError("couchdb unreachable")
        if request.method == "GET" and tail in ("", "/"):
            return httpx.Response(200, json={"update_seq": str(self.seq())})
        if request.method == "GET" and tail == "/_changes":
            since = int(request.url.params["since"])
            if self.on_changes:
                self.on_changes(since)
            limit = int(request.url.params["limit"])
            latest = {}
            for seq, doc_id, deleted in self.log:
                if seq > since:
                    latest.pop(doc_id, None)  # CouchDB reports each id once, at its latest seq
                    latest[doc_id] = (seq, deleted)
            rows = []
            ordered = sorted(latest.items(), key=lambda kv: kv[1][0])
            pending = max(0, len(ordered) - limit)
            for doc_id, (seq, deleted) in ordered[:limit]:
                row = {"seq": str(seq), "id": doc_id, "changes": [{"rev": "x"}]}
                if deleted:
                    row["deleted"] = True
                    row["doc"] = {"_id": doc_id, "_rev": "9-d", "_deleted": True}
                else:
                    row["doc"] = dict(self.docs[doc_id])
                rows.append(row)
            last = rows[-1]["seq"] if rows else str(since)
            return httpx.Response(200, json={"results": rows, "last_seq": last, "pending": pending})
        if request.method == "GET" and tail == "/_all_docs":
            params = request.url.params
            start = json.loads(params["startkey"]) if "startkey" in params else ""
            end = json.loads(params["endkey"]) if "endkey" in params else None
            rows = [{"id": i, "key": i, "doc": dict(d)} for i, d in sorted(self.docs.items())
                    if i >= start and (end is None or i < end)]
            return httpx.Response(200, json={"rows": rows})
        if request.method == "POST" and tail == "/_all_docs":
            rows = []
            for k in json.loads(request.content)["keys"]:
                if k in self.docs:
                    rows.append({"id": k, "key": k, "value": {"rev": "1"}, "doc": dict(self.docs[k])})
                else:
                    rows.append({"key": k, "error": "not_found"})
            return httpx.Response(200, json={"rows": rows})
        raise AssertionError(f"unexpected request {request.method} {request.url}")


class FakeClient(ObsidianVaultClient):
    def __init__(self, couch: FakeCouch, db_name: str = DB):
        super().__init__(Config(couch_url="http://fake", db_name=db_name))
        self._client = httpx.AsyncClient(
            base_url=f"http://fake/{DB}", transport=httpx.MockTransport(couch.handler), timeout=30.0
        )


def run(coro):
    return asyncio.run(coro)


def _built(tmp_path, couch):
    out = tmp_path / "idx.sqlite"
    run(si.build(FakeClient(couch), out))
    return out


def _cycle(out, couch, clock=None):
    conn = si.connect(out)
    si.create_schema(conn)
    try:
        kw = {"clock": clock} if clock else {}
        return run(si.sync_once(FakeClient(couch), conn, **kw))
    finally:
        conn.close()


def _match(out, phrase):
    conn = sqlite3.connect(out)
    try:
        return sorted(r[0] for r in conn.execute(
            "SELECT n.path FROM notes_fts f JOIN notes n ON n.rowid = f.rowid"
            " WHERE notes_fts MATCH ?", (json.dumps(phrase),)))
    finally:
        conn.close()


def _rows(out):
    conn = sqlite3.connect(out)
    try:
        notes = conn.execute("SELECT * FROM notes ORDER BY id").fetchall()
        fts = conn.execute("SELECT rowid, body FROM notes_fts ORDER BY rowid").fetchall()
        retry = conn.execute("SELECT * FROM retry ORDER BY id").fetchall()
        return notes, fts, retry
    finally:
        conn.close()


def _meta(out):
    conn = sqlite3.connect(out)
    try:
        return si.get_meta(conn)
    finally:
        conn.close()


def _seeded():
    couch = FakeCouch()
    couch.put_chunk("h:a1", "the quick brown fox")
    couch.put_note("A.md", ["h:a1"])
    return couch


@pytest.fixture
def out_and_couch(tmp_path):
    couch = _seeded()
    return _built(tmp_path, couch), couch


def test_new_note_is_searchable_after_one_cycle(out_and_couch):
    out, couch = out_and_couch
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("Notes/B.md", ["h:b1"])
    counts = _cycle(out, couch)
    assert counts["put_ok"] == 1
    assert _match(out, "zebra") == ["Notes/B.md"]


def test_edit_replaces_old_text(out_and_couch):
    out, couch = out_and_couch
    couch.put_chunk("h:a2", "a lazy dog")
    couch.put_note("A.md", ["h:a2"], rev="2-b")
    _cycle(out, couch)
    assert _match(out, "brown fox") == []
    assert _match(out, "lazy dog") == ["A.md"]


def test_body_deleted_flag_removes_note(out_and_couch):
    out, couch = out_and_couch
    couch.put_note("A.md", ["h:a1"], rev="2-b", deleted=True)
    counts = _cycle(out, couch)
    assert counts["removed"] == 1
    assert _match(out, "brown fox") == []
    assert _rows(out)[0] == []


def test_couchdb_level_delete_removes_note(out_and_couch):
    out, couch = out_and_couch
    couch.couch_delete("a.md")
    counts = _cycle(out, couch)
    assert counts["removed"] == 1
    assert _match(out, "brown fox") == [] and _rows(out)[0] == []


def test_excluded_extension_is_not_indexed(out_and_couch):
    out, couch = out_and_couch
    couch.put_chunk("h:js", "var zebra = 1;")
    couch.put_note("Plugin.js", ["h:js"])
    counts = _cycle(out, couch)
    assert counts["put_ok"] == 0 and counts["removed"] == 0
    assert _match(out, "zebra") == []


def test_rename_to_excluded_extension_removes_indexed_note(out_and_couch):
    out, couch = out_and_couch
    # Same id rewritten with an excluded extension: the old row must go.
    couch.put_note("A.md", ["h:a1"], rev="2-b")
    couch.docs["a.md"]["path"] = "A.png"
    counts = _cycle(out, couch)
    assert counts["removed"] == 1
    assert _match(out, "brown fox") == []


def test_non_note_ids_are_ignored(out_and_couch):
    out, couch = out_and_couch
    couch.docs["_design/x"] = {"_id": "_design/x", "_rev": "1"}
    couch.log.append((couch.seq() + 1, "_design/x", False))
    couch.docs["ix:abc"] = {"_id": "ix:abc", "_rev": "1", "type": "plain", "path": "Z.md",
                            "children": []}
    couch.log.append((couch.seq() + 1, "ix:abc", False))
    before = _rows(out)
    counts = _cycle(out, couch)
    assert counts["put_ok"] + counts["removed"] + counts["put_failed"] == 0
    assert _rows(out) == before


def test_missing_chunk_fails_then_heals_when_chunk_arrives(out_and_couch):
    out, couch = out_and_couch
    now = [10_000]
    couch.put_chunk("h:c1", "first half ")
    couch.put_note("C.md", ["h:c1", "h:c2"])  # h:c2 not written yet
    counts = _cycle(out, couch, clock=lambda: now[0])
    assert counts["put_failed"] == 1
    notes, fts, retry = _rows(out)
    status = {n[1]: n[8] for n in notes}
    assert status["c.md"] == "failed"
    assert _match(out, "first half") == []  # partial text is never indexed
    assert [(r[0], json.loads(r[3])) for r in retry] == [("c.md", ["h:c2"])]
    # Well past the fast window, so only the arrived chunk can make it due.
    now[0] += si.RETRY_FAST_WINDOW + 5
    couch.put_chunk("h:c2", "second half")
    counts = _cycle(out, couch, clock=lambda: now[0])
    assert counts["retried"] == 1 and counts["put_ok"] == 1
    assert _match(out, "first half second half") == ["C.md"]
    assert _rows(out)[2] == []  # retry row cleared


def test_failed_note_backs_off_to_hourly_after_fast_window(out_and_couch):
    out, couch = out_and_couch
    now = [10_000]
    couch.put_note("D.md", ["h:gone"])
    _cycle(out, couch, clock=lambda: now[0])
    now[0] += 60
    assert _cycle(out, couch, clock=lambda: now[0])["retried"] == 1  # inside fast window
    now[0] += si.RETRY_FAST_WINDOW  # past the window, 600 s after the last try
    assert _cycle(out, couch, clock=lambda: now[0])["retried"] == 0
    now[0] += si.RETRY_SLOW_INTERVAL - si.RETRY_FAST_WINDOW - 1
    assert _cycle(out, couch, clock=lambda: now[0])["retried"] == 0
    now[0] += 1  # exactly an hour since the last try
    assert _cycle(out, couch, clock=lambda: now[0])["retried"] == 1


def test_unrelated_chunk_does_not_wake_a_backed_off_note(out_and_couch):
    out, couch = out_and_couch
    now = [10_000]
    couch.put_note("D.md", ["h:gone"])
    _cycle(out, couch, clock=lambda: now[0])
    now[0] += si.RETRY_FAST_WINDOW + 5
    couch.put_chunk("h:other", "unrelated")
    assert _cycle(out, couch, clock=lambda: now[0])["retried"] == 0


def test_retry_of_deleted_failed_note_removes_it(out_and_couch):
    out, couch = out_and_couch
    couch.put_note("D.md", ["h:gone"])
    _cycle(out, couch)
    # Deleted at the CouchDB level; the retry re-read finds it gone.
    del couch.docs["d.md"]
    counts = _cycle(out, couch)
    assert counts["retried"] == 1 and counts["removed"] == 1
    notes, _, retry = _rows(out)
    assert "d.md" not in [n[1] for n in notes] and retry == []


def test_replaying_a_batch_leaves_identical_rows(out_and_couch):
    out, couch = out_and_couch
    since = _meta(out)["last_seq"]
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("B.md", ["h:b1"])
    couch.put_chunk("h:a2", "a lazy dog")
    couch.put_note("A.md", ["h:a2"], rev="2-b")
    _cycle(out, couch)
    first = _rows(out)
    # Rewind the sequence: the same batch is delivered again, as it is after a
    # build whose pre-listing seq precedes changes the listing already held.
    conn = sqlite3.connect(out)
    with conn:
        si.set_meta(conn, last_seq=since)
    conn.close()
    counts = _cycle(out, couch)
    assert counts["changes"] > 0 and counts["put_ok"] == 0
    assert _rows(out) == first


def test_last_seq_and_heartbeat_advance(out_and_couch):
    out, couch = out_and_couch
    start = _meta(out)
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("B.md", ["h:b1"])
    _cycle(out, couch, clock=lambda: 2_000_000_000)
    after = _meta(out)
    assert int(after["last_seq"]) == couch.seq() > int(start["last_seq"])
    assert after["heartbeat_at"] == "2000000000"
    # An empty cycle still beats.
    _cycle(out, couch, clock=lambda: 2_000_000_060)
    assert _meta(out)["heartbeat_at"] == "2000000060"
    assert _meta(out)["last_seq"] == after["last_seq"]


def test_transport_error_changes_nothing_and_follow_survives(out_and_couch, capsys):
    out, couch = out_and_couch
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("B.md", ["h:b1"])
    before_rows, before_meta = _rows(out), _meta(out)

    async def go():
        stop = asyncio.Event()
        couch.fail = True
        client = FakeClient(couch)
        # Fail every request; stop once the first failure has been logged.
        task = asyncio.ensure_future(si.follow(client, out, stop_event=stop))
        err = ""
        for _ in range(200):
            await asyncio.sleep(0.01)
            err += capsys.readouterr().err
            if "couchdb unreachable" in err:
                break
        stop.set()
        await task  # returns normally, no exception
        return err

    err = run(go())
    assert "follow: couchdb unreachable (ConnectError: couchdb unreachable); retrying in 1s" in err
    assert _rows(out) == before_rows
    assert _meta(out) == before_meta


def test_transport_error_mid_cycle_writes_nothing(out_and_couch):
    out, couch = out_and_couch
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("B.md", ["h:b1"])
    before_rows, before_meta = _rows(out), _meta(out)
    real = couch.handler

    def fail_on_chunk_fetch(request):
        if request.method == "POST":
            raise httpx.ReadTimeout("slow")
        return real(request)

    client = FakeClient(couch)
    client._client = httpx.AsyncClient(base_url=f"http://fake/{DB}",
                                       transport=httpx.MockTransport(fail_on_chunk_fetch))
    conn = si.connect(out)
    with pytest.raises(httpx.TransportError):
        run(si.sync_once(client, conn))
    conn.close()
    assert _rows(out) == before_rows and _meta(out) == before_meta


def _run_follow_until_caught_up(out, couch):
    """Run follow until it asks for changes past the current tip, then stop."""
    async def go():
        stop = asyncio.Event()
        tip = couch.seq()

        def maybe_stop(since):
            if since >= tip:
                stop.set()
        couch.on_changes = maybe_stop
        await si.follow(FakeClient(couch), out, stop_event=stop)

    run(go())


def test_restart_resumes_without_build(out_and_couch, monkeypatch, capsys):
    out, couch = out_and_couch

    async def no_build(*a, **k):
        raise AssertionError("build must not run on a normal restart")

    monkeypatch.setattr(si, "build", no_build)
    couch.put_chunk("h:b1", "a zebra crossing")
    couch.put_note("B.md", ["h:b1"])
    _run_follow_until_caught_up(out, couch)
    assert _match(out, "zebra") == ["B.md"]
    err = capsys.readouterr().err
    assert "follow: resumed" in err and "put ok=1" in err


def test_schema_version_mismatch_triggers_build(out_and_couch, monkeypatch, capsys):
    out, couch = out_and_couch
    conn = sqlite3.connect(out)
    with conn:
        si.set_meta(conn, schema_version="0")
    conn.close()
    calls = []
    real_build = si.build

    async def spy(client, path):
        calls.append(path)
        return await real_build(client, path)

    monkeypatch.setattr(si, "build", spy)
    _run_follow_until_caught_up(out, couch)
    assert calls == [out]
    assert _meta(out)["schema_version"] == si.SCHEMA_VERSION
    assert "follow: built" in capsys.readouterr().err


def test_other_database_or_missing_file_triggers_build(tmp_path, monkeypatch):
    couch = _seeded()
    assert not si.index_is_current(tmp_path / "none.sqlite", DB)
    out = _built(tmp_path, couch)
    assert si.index_is_current(out, DB)
    assert not si.index_is_current(out, "some-other-db")


def test_stop_during_longpoll_returns_promptly(out_and_couch):
    out, couch = out_and_couch
    real = couch.handler

    async def go():
        stop = asyncio.Event()

        async def slow_handler(request):
            if request.url.path.endswith("/_changes"):
                await asyncio.sleep(30)  # a quiet longpoll
            return real(request)

        client = FakeClient(couch)
        client._client = httpx.AsyncClient(base_url=f"http://fake/{DB}",
                                           transport=httpx.MockTransport(slow_handler))
        task = asyncio.ensure_future(si.follow(client, out, stop_event=stop))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, timeout=2)

    before = _meta(out)
    run(go())
    assert _meta(out) == before
