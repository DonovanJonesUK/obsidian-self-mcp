"""Tests for the fixes from the 2026-10-01 cross-vendor review of the search index.

Each test names the finding it guards. The defects were all quiet ones: a
rebuild shadowed by the old file's WAL, an empty query with no bound on its
work, a fallback whose reason vanished when the scan also failed, a lagging
index reported as current, a malformed doc crash-looping the follower, retries
with no per-cycle cap, Bases indexed as base64, and case-folding misses.
"""

import asyncio
import base64
import sqlite3
from pathlib import Path

import pytest

from obsidian_self_mcp import search_fold
from obsidian_self_mcp import search_index as si
from obsidian_self_mcp import search_query as sq
from obsidian_self_mcp.client import ObsidianVaultClient, SearchFallbackError
from obsidian_self_mcp.config import Config

from test_search_index_follow import DB, FakeClient, FakeCouch, _built, _cycle, _meta, _seeded, run
from test_search_query import ask, make_index


def _row(out, doc_id):
    conn = sqlite3.connect(out)
    try:
        return conn.execute(
            "SELECT status, reason, fold_odd FROM notes WHERE id = ?", (doc_id,)
        ).fetchone()
    finally:
        conn.close()


# Finding 1: a rebuild must not be shadowed by the replaced file's WAL.

def _crashed_writer_files(tmp_path):
    """Main file and WAL bytes as a SIGKILLed writer leaves them."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    db = scratch / "old.sqlite"
    old = si.connect(db)
    si.create_schema(old)
    old.execute("PRAGMA wal_autocheckpoint=0")
    with old:
        for i in range(300):
            old.execute(
                "INSERT INTO notes(id, path, path_lc, ext, rev, children_key, status)"
                " VALUES(?, ?, ?, 'md', '1', 'k', 'ok')", (f"o{i}", f"o{i}.md", f"o{i}.md"))
        si.set_meta(old, schema_version="0", last_seq="old")
    files = db.read_bytes(), Path(f"{db}-wal").read_bytes()
    old.close()  # a clean close would checkpoint; the bytes above are the crash state
    return files


def _plant(path, files):
    path.write_bytes(files[0])
    Path(f"{path}-wal").write_bytes(files[1])
    Path(f"{path}-shm").unlink(missing_ok=True)


def test_build_discards_a_stale_wal_left_by_a_crashed_writer(tmp_path):
    files = _crashed_writer_files(tmp_path)
    probe = tmp_path / "probe.sqlite"
    _plant(probe, files)
    conn = sqlite3.connect(probe)
    assert si.get_meta(conn)["last_seq"] == "old"  # the planted WAL is live
    conn.close()

    out = tmp_path / "idx.sqlite"
    _plant(out, files)
    couch = _seeded()
    with si.index_lock(out):
        run(si.build(FakeClient(couch), out))
    meta = _meta(out)
    assert meta["schema_version"] == si.SCHEMA_VERSION
    assert meta["last_seq"] == str(couch.seq())


def test_index_lock_refuses_a_second_holder(tmp_path):
    out = tmp_path / "idx.sqlite"
    with si.index_lock(out):
        with pytest.raises(si.IndexBusy, match="held by another process"):
            with si.index_lock(out):
                pass
    with si.index_lock(out):  # released on exit
        pass


def test_cli_build_refuses_while_the_follower_runs(tmp_path):
    out = _built(tmp_path, _seeded())
    couch = _seeded()

    async def go():
        stop = asyncio.Event()
        follower = asyncio.ensure_future(si.follow(FakeClient(couch), out, stop_event=stop))
        await asyncio.sleep(0.2)
        try:
            with pytest.raises(si.IndexBusy):
                with si.index_lock(out):
                    pass
        finally:
            stop.set()
            await follower

    run(go())


# Finding 2: an empty query is refused; counting never materialises matches.

@pytest.mark.parametrize("query", ["", "   ", "\n\t"])
def test_empty_query_is_refused_by_the_index_path(tmp_path, query):
    p = make_index(tmp_path, {"a.md": "anything"})
    with pytest.raises(ValueError, match="empty"):
        ask(p, query)


def test_empty_query_is_refused_before_any_scan(monkeypatch, tmp_path):
    client = ObsidianVaultClient(Config(couch_url="http://x", db_name=DB))
    calls = []

    async def fake_scan(*a, **k):
        calls.append(a)
        return []

    monkeypatch.setattr(client, "_search_notes_scan", fake_scan)
    with pytest.raises(ValueError, match="empty"):
        asyncio.run(client.search_notes_report("  "))
    assert calls == []


def test_one_letter_query_counts_every_occurrence(tmp_path):
    p = make_index(tmp_path, {"a.md": "e" * 50_000, "b.md": "xex"})
    answer = ask(p, "e")
    assert [(r.path, r.matches) for r in answer.results] == [("a.md", 50_000), ("b.md", 1)]
    assert len(answer.results[0].snippets) == sq.SNIPPETS_PER_NOTE


# Finding 3: when the fallback scan fails, the reason still leads the error.

def test_failed_fallback_keeps_the_reason_on_line_one(monkeypatch, tmp_path):
    client = ObsidianVaultClient(Config(couch_url="http://x", db_name=DB))

    async def failing_scan(*a, **k):
        raise RuntimeError("scan exploded")

    monkeypatch.setattr(client, "_search_notes_scan", failing_scan)
    with pytest.raises(SearchFallbackError) as info:
        asyncio.run(client.search_notes_report("term"))
    first_line = str(info.value).splitlines()[0]
    assert first_line.startswith("index unavailable (index file missing); fell back to live scan")
    assert "scan exploded" in str(info.value)


# Finding 4: an index far behind CouchDB is not reported as current.

def test_follower_records_pending_and_reader_refuses_a_large_backlog(tmp_path, monkeypatch):
    out = _built(tmp_path, _seeded())
    couch = _seeded()
    for i in range(5):
        couch.put_chunk(f"h:p{i}", f"text {i}")
        couch.put_note(f"P{i}.md", [f"h:p{i}"])
    monkeypatch.setattr(si, "CHANGES_LIMIT", 4)
    _cycle(out, couch)
    assert int(_meta(out)["pending"]) > 0
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_MAX_PENDING", "1")
    conn = sqlite3.connect(out)
    try:
        with pytest.raises(sq.IndexUnavailable, match="changes behind"):
            sq.check_available(conn, DB)
    finally:
        conn.close()
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_MAX_PENDING", "100")
    conn = sqlite3.connect(out)
    try:
        sq.check_available(conn, DB)
    finally:
        conn.close()


def test_changes_response_without_pending_is_refused(tmp_path):
    out = _built(tmp_path, _seeded())
    couch = _seeded()
    original = couch.handler

    def no_pending(request):
        resp = original(request)
        if request.url.path.endswith("/_changes"):
            body = resp.json()
            body.pop("pending")
            return type(resp)(200, json=body)
        return resp

    couch.handler = no_pending
    before = _meta(out)
    with pytest.raises(RuntimeError, match="pending"):
        _cycle(out, couch)
    assert _meta(out) == before


# Finding 5: a malformed doc becomes a failed row instead of crashing.

@pytest.mark.parametrize("children", [None, [1, 2], "h:a1", {"a": 1}])
def test_malformed_children_fail_the_note_not_the_follower(out_and_couch, children):
    out, couch = out_and_couch
    couch.docs["bad.md"] = {"_id": "bad.md", "path": "bad.md", "type": "plain",
                            "children": children, "_rev": "1-b"}
    couch.log.append((couch.seq() + 1, "bad.md", False))
    _cycle(out, couch)
    status, reason, _ = _row(out, "bad.md")
    assert status == "failed" and "malformed" in reason


def test_non_string_path_fails_the_note(out_and_couch):
    out, couch = out_and_couch
    couch.put_chunk("h:z", "zzz")
    couch.docs["odd.md"] = {"_id": "odd.md", "path": 42, "type": "plain",
                            "children": ["h:z"], "_rev": "1-b"}
    couch.log.append((couch.seq() + 1, "odd.md", False))
    _cycle(out, couch)
    status, reason, _ = _row(out, "odd.md")
    assert status == "failed" and "path" in reason


@pytest.fixture
def out_and_couch(tmp_path):
    couch = _seeded()
    return _built(tmp_path, couch), couch


# Finding 6: retries due in one cycle are capped, oldest first.

def test_due_retries_are_capped_oldest_first(tmp_path, monkeypatch):
    out = tmp_path / "idx.sqlite"
    conn = si.connect(out)
    si.create_schema(conn)
    with conn:
        for i in range(6):
            si.put_note(conn, {"_id": f"f{i}", "path": f"f{i}.md", "children": ["h:gone"]}, {})
        conn.execute("INSERT INTO retry VALUES('f0', 1, 50, '[]')")
        conn.execute("INSERT INTO retry VALUES('f1', 1, 10, '[]')")
    monkeypatch.setattr(si, "RETRY_MAX_PER_CYCLE", 3)
    due = si.due_retries(conn, now=100, arrived=set(), exclude=set())
    conn.close()
    # f2..f5 have never been tried, so they come first; then the oldest try.
    assert due == ["f2", "f3", "f4"]


# Finding 7: a newnote doc (Bases) is indexed as its decoded text.

def test_newnote_is_indexed_decoded(tmp_path):
    text = "filters:\n  and:\n    - status == \"raised\"\n"
    encoded = base64.b64encode(text.encode()).decode()
    couch = FakeCouch()
    couch.put_chunk("h:b1", encoded[:10])
    couch.put_chunk("h:b2", encoded[10:])
    couch.put_note("Views/Tickets.base", ["h:b1", "h:b2"], type="newnote")
    out = _built(tmp_path, couch)
    assert _row(out, "views/tickets.base")[0] == "ok"
    conn = sqlite3.connect(out)
    try:
        body = conn.execute("SELECT body FROM notes_fts").fetchone()[0]
    finally:
        conn.close()
    assert body == text


def test_newnote_that_is_not_utf8_text_fails(tmp_path):
    couch = FakeCouch()
    couch.put_chunk("h:x", base64.b64encode(b"\xff\xfe\x00binary").decode())
    couch.put_note("Views/Broken.base", ["h:x"], type="newnote")
    out = _built(tmp_path, couch)
    status, reason, _ = _row(out, "views/broken.base")
    assert status == "failed" and "UTF-8" in reason


# Finding 8: no hit is missed where SQLite and Python fold case differently.

def test_ascii_query_finds_dotted_capital_i(tmp_path):
    p = make_index(tmp_path, {"t.md": "a trip to İstanbul", "u.md": "istanbul again"})
    assert {r.path for r in ask(p, "istanbul").results} == {"t.md", "u.md"}


def test_dotted_capital_i_query_finds_ascii(tmp_path):
    p = make_index(tmp_path, {"u.md": "istanbul again", "v.md": "nothing"})
    assert [r.path for r in ask(p, "İstanbul").results] == ["u.md"]


def test_fold_odd_flag_is_set_only_where_needed(tmp_path):
    p = make_index(tmp_path, {"t.md": "İstanbul", "u.md": "plain ascii"})
    conn = sqlite3.connect(p)
    try:
        flags = dict(conn.execute("SELECT path, fold_odd FROM notes"))
    finally:
        conn.close()
    assert flags == {"t.md": 1, "u.md": 0}


def test_fold_tables_match_the_running_python_and_sqlite():
    # About 8 s: walks every code point. Runs every time on purpose; it is what
    # catches a Python or SQLite upgrade moving either folding.
    odd, scan = search_fold.compute_fold_sets()
    assert odd == search_fold.fold_odd_codepoints()
    assert {chr(c) for c in scan} == set(search_fold.SCAN_QUERY_CHARS)


# Finding 9: one or two non-ASCII characters still match case-insensitively.

def test_short_non_ascii_query_matches_other_case(tmp_path):
    p = make_index(tmp_path, {"a.md": "École", "b.md": "ecole"})
    assert [r.path for r in ask(p, "é").results] == ["a.md"]


def test_short_ascii_query_matches_kelvin_sign(tmp_path):
    p = make_index(tmp_path, {"a.md": "5 K", "b.md": "none"})
    assert [r.path for r in ask(p, "k").results] == ["a.md"]


def test_short_ascii_query_matches_long_s_and_dotless_i(tmp_path):
    p = make_index(tmp_path, {"a.md": "xſ", "b.md": "xı", "c.md": "xy"})
    assert [r.path for r in ask(p, "xs").results] == ["a.md"]
    assert [r.path for r in ask(p, "xi").results] == ["b.md"]


def test_ascii_equivalents_table_matches_the_running_python():
    assert search_fold.compute_ascii_equivalents() == search_fold.ASCII_EQUIVALENTS


def test_short_ascii_query_is_narrowed_by_like(tmp_path):
    sql, _ = sq._candidate_sql("Q3", None)
    assert "LIKE" in sql and "GLOB" not in sql
    sql, _ = sq._candidate_sql("k", None)
    assert "LIKE" in sql and "GLOB" in sql


# Finding 10: wrong defaults and silently ignored settings.

def test_limit_below_one_is_refused(tmp_path):
    p = make_index(tmp_path, {"a.md": "term"})
    with pytest.raises(ValueError, match="limit"):
        ask(p, "term", limit=0)


@pytest.mark.parametrize("value", ["abc", "0", "-5"])
def test_bad_stale_threshold_is_announced_not_ignored(tmp_path, monkeypatch, value):
    p = make_index(tmp_path, {"a.md": "term"})
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_STALE_SECONDS", value)
    with pytest.raises(sq.IndexUnavailable, match="OBSIDIAN_SEARCH_INDEX_STALE_SECONDS"):
        ask(p, "term")


@pytest.mark.parametrize("value", ["0", "false", "OFF", "no"])
def test_switch_off_accepts_the_usual_words(monkeypatch, value):
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX", value)
    assert sq.index_disabled_reason() == f"index disabled (OBSIDIAN_SEARCH_INDEX={value})"


@pytest.mark.parametrize("value", ["1", "true", "on", "yes", ""])
def test_switch_on_accepts_the_usual_words(monkeypatch, value):
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX", value)
    assert sq.index_disabled_reason() is None


def test_unrecognised_switch_value_is_announced(monkeypatch):
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX", "maybe")
    assert "not a recognised" in sq.index_disabled_reason()


def test_changed_extension_list_forces_a_rebuild(tmp_path, monkeypatch):
    out = _built(tmp_path, _seeded())
    assert si.index_is_current(out, DB)
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_EXTS", "md")
    assert not si.index_is_current(out, DB)


def test_index_for_another_server_is_not_used(tmp_path):
    out = _built(tmp_path, _seeded())
    assert si.index_is_current(out, DB, si.couch_origin("http://fake"))
    assert not si.index_is_current(out, DB, si.couch_origin("http://elsewhere:5984"))
    conn = sqlite3.connect(out)
    try:
        with pytest.raises(sq.IndexUnavailable, match="server"):
            sq.check_available(conn, DB, si.couch_origin("http://elsewhere:5984"))
    finally:
        conn.close()


def test_couch_origin_drops_credentials_and_path():
    assert si.couch_origin("http://user:secret@host:5984/db") == "http://host:5984"
    assert "secret" not in si.couch_origin("https://u:secret@h")
