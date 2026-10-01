"""Tests for the search index query path and its announced fallback (SAI-DEC-240).

Each guard here protects against a silent wrong answer: a stale index read as
current, an index for another database, a folder prefix matching a sibling, a
note with a missing chunk counted as searched, or a trigram match reported
without confirming the text really contains the query.
"""

import asyncio
import time

import pytest

from obsidian_self_mcp import search_index as si
from obsidian_self_mcp import search_query as sq
from obsidian_self_mcp.client import ObsidianVaultClient
from obsidian_self_mcp.config import Config

DB = "fake-test-db"


def make_index(tmp_path, notes, *, heartbeat_age=0, db_name=DB, schema=None):
    path = tmp_path / "idx.sqlite"
    conn = si.connect(path)
    si.create_schema(conn)
    with conn:
        for i, (note_path, body) in enumerate(notes.items()):
            doc = {"_id": note_path.lower(), "path": note_path, "children": [f"c{i}"]}
            chunks = {} if body is None else {f"c{i}": body}
            si.put_note(conn, doc, chunks)
        si.set_meta(
            conn,
            schema_version=schema or si.SCHEMA_VERSION,
            fold_version=si.FOLD_VERSION,
            db_name=db_name,
            couch_origin=si.couch_origin("http://x"),
            last_seq="1-x",
            heartbeat_at=int(time.time()) - heartbeat_age,
            note_count=len(notes),
            exts="md",
        )
    conn.close()
    return path


def ask(path, query, folder=None, limit=20):
    return sq.query_index(DB, query, folder, limit, path=path)


def test_finds_note_ranks_by_occurrences_then_path(tmp_path):
    p = make_index(tmp_path, {"B.md": "cat cat", "A.md": "cat cat", "C.md": "cat cat cat", "D.md": "dog"})
    got = ask(p, "cat")
    assert [(r.path, r.matches) for r in got.results] == [("C.md", 3), ("A.md", 2), ("B.md", 2)]


@pytest.mark.parametrize("query", ['say "hi"', '"abc', 'x" OR "y', "[[SAI-DEC-211]]", "a.b*c", "---", "(x|y)"])
def test_phrase_escaping_matches_literally(tmp_path, query):
    p = make_index(tmp_path, {"hit.md": f"before {query} after", "miss.md": "nothing here at all"})
    assert [r.path for r in ask(p, query).results] == ["hit.md"]


def test_regex_metacharacters_are_not_a_pattern(tmp_path):
    p = make_index(tmp_path, {"n.md": "abbbc"})
    assert ask(p, "ab+c").results == []


def test_short_query_uses_like_and_escapes_wildcards(tmp_path):
    p = make_index(tmp_path, {"a.md": "100% done", "b.md": "plain text", "c.md": "snake_case"})
    assert [r.path for r in ask(p, "%").results] == ["a.md"]
    assert [r.path for r in ask(p, "_").results] == ["c.md"]
    assert {r.path for r in ask(p, "ai").results} == {"b.md"}


def test_case_insensitive_and_non_ascii(tmp_path):
    p = make_index(tmp_path, {"n.md": "citing Émile Durkheim", "m.md": "straße"})
    assert [r.path for r in ask(p, "émile").results] == ["n.md"]
    assert [r.path for r in ask(p, "ÉMILE").results] == ["n.md"]
    assert [r.path for r in ask(p, "é").results] == ["n.md"]


def test_folder_boundary_does_not_match_sibling_prefix(tmp_path):
    p = make_index(tmp_path, {"Projects/SAI/a.md": "term", "Projects/SAI2/b.md": "term", "Projects/sai/c.md": "term"})
    assert {r.path for r in ask(p, "term", folder="Projects/SAI").results} == {
        "Projects/SAI/a.md", "Projects/sai/c.md"
    }


def test_folder_applies_before_limit(tmp_path):
    notes = {f"Other/{i}.md": "term term term" for i in range(5)}
    notes["Wanted/x.md"] = "term"
    p = make_index(tmp_path, notes)
    assert [r.path for r in ask(p, "term", folder="Wanted", limit=1).results] == ["Wanted/x.md"]


def test_trigram_overselection_is_confirmed_in_python(tmp_path):
    p = make_index(tmp_path, {"n.md": "abc def"})
    assert ask(p, "abcdef").results == []


@pytest.mark.parametrize("folder", [None, "projects/"])
def test_match_runs_once_not_once_per_note(tmp_path, folder):
    # A plan that drove from notes re-ran the MATCH per note: 8 to 23 s on
    # production. FTS5 marks a MATCH constraint with M in the plan's index
    # string, so it must appear exactly once, and never on a row lookup.
    p = make_index(tmp_path, {f"n{i}.md": f"body {i}" for i in range(50)})
    conn = sq._open_ro(p)
    sql, args = sq._candidate_sql("body", folder)
    plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, args)]
    conn.close()
    fts = [r.split("VIRTUAL TABLE INDEX ", 1)[1] for r in plan if "VIRTUAL TABLE INDEX" in r]
    assert [i for i in fts if "M" in i] == ["0:M1"], plan
    assert all("=" not in i for i in fts if "M" in i), plan


def test_missing_chunk_note_is_listed_not_searched(tmp_path):
    p = make_index(tmp_path, {"ok.md": "term", "gap.md": None})
    got = ask(p, "term")
    assert [r.path for r in got.results] == ["ok.md"]
    assert got.failed_in_scope == ["gap.md"]
    assert "gap.md" in sq.format_failed(got.failed_in_scope)


def test_failed_notes_are_scoped_to_the_folder(tmp_path):
    p = make_index(tmp_path, {"In/gap.md": None, "Out/gap.md": None, "In/ok.md": "term"})
    assert ask(p, "term", folder="In").failed_in_scope == ["In/gap.md"]


def test_snippet_shape_matches_legacy(tmp_path):
    body = "x" * 100 + "needle" + "y" * 100
    p = make_index(tmp_path, {"n.md": body})
    snip = ask(p, "needle").results[0].snippets[0]
    assert snip == "..." + "x" * 60 + "needle" + "y" * 60 + "..."


def test_footer_reports_counts_and_heartbeat(tmp_path):
    p = make_index(tmp_path, {"a.md": "term"}, heartbeat_age=7)
    footer = ask(p, "term").footer
    assert "1 notes" in footer and "1 matched" in footer and "heartbeat 7s ago" in footer


@pytest.mark.parametrize(
    "kwargs,fragment",
    [
        ({"heartbeat_age": 10 * 60}, "heartbeat 10 min old"),
        ({"schema": "999"}, "schema_version"),
        ({"db_name": "another-db"}, "another-db"),
    ],
)
def test_unavailable_index_raises_with_reason(tmp_path, kwargs, fragment):
    p = make_index(tmp_path, {"a.md": "term"}, **kwargs)
    with pytest.raises(sq.IndexUnavailable, match=fragment):
        ask(p, "term")


def test_missing_and_corrupt_file_raise(tmp_path):
    with pytest.raises(sq.IndexUnavailable, match="missing"):
        ask(tmp_path / "nope.sqlite", "term")
    bad = tmp_path / "bad.sqlite"
    bad.write_bytes(b"this is not a database" * 100)
    with pytest.raises(sq.IndexUnavailable, match="unreadable"):
        ask(bad, "term")


def test_stale_threshold_is_configurable(tmp_path, monkeypatch):
    p = make_index(tmp_path, {"a.md": "term"}, heartbeat_age=400)
    with pytest.raises(sq.IndexUnavailable):
        ask(p, "term")
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_STALE_SECONDS", "900")
    assert [r.path for r in ask(p, "term").results] == ["a.md"]


def make_client(monkeypatch, tmp_path, scan_result="SCAN"):
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_DIR", str(tmp_path))
    cfg = Config(couch_url="http://x", couch_user="u", couch_pass="p", db_name=DB)
    client = ObsidianVaultClient(cfg)
    calls = []

    async def fake_scan(query, folder=None, limit=20):
        calls.append(query)
        return scan_result

    monkeypatch.setattr(client, "_search_notes_scan", fake_scan)
    return client, calls


def test_report_falls_back_with_announced_first_line_when_index_missing(monkeypatch, tmp_path):
    client, calls = make_client(monkeypatch, tmp_path)
    report = asyncio.run(client.search_notes_report("term"))
    assert report.notice == "index unavailable (index file missing); fell back to live scan"
    assert report.results == "SCAN" and calls == ["term"]


def test_report_falls_back_when_heartbeat_stale(monkeypatch, tmp_path):
    client, calls = make_client(monkeypatch, tmp_path)
    make_index(tmp_path, {"a.md": "term"}, heartbeat_age=42 * 60).rename(si.default_index_path(DB))
    report = asyncio.run(client.search_notes_report("term"))
    assert report.notice == "index unavailable (heartbeat 42 min old); fell back to live scan"
    assert calls == ["term"]


def test_report_uses_index_when_current_and_does_not_scan(monkeypatch, tmp_path):
    client, calls = make_client(monkeypatch, tmp_path)
    make_index(tmp_path, {"a.md": "term"}).rename(si.default_index_path(DB))
    report = asyncio.run(client.search_notes_report("term"))
    assert report.notice is None and calls == []
    assert [r.path for r in report.results] == ["a.md"] and report.footer


def test_switch_off_forces_the_scan_and_says_so(monkeypatch, tmp_path):
    client, calls = make_client(monkeypatch, tmp_path)
    make_index(tmp_path, {"a.md": "term"}).rename(si.default_index_path(DB))
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX", "0")
    report = asyncio.run(client.search_notes_report("term"))
    assert report.notice == "index disabled (OBSIDIAN_SEARCH_INDEX=0); fell back to live scan"
    assert calls == ["term"]
