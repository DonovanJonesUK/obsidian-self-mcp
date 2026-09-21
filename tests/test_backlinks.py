"""Tests for get_backlinks and the rename_note paths that consume it.

The defects these guard against are silent ones: a note left out of a backlink
scan without anyone being told, a gap written back as permanent content, and a
cache that keeps answering after the note under it changed. Each case asserts
the branch fires, not merely that nothing crashed.
"""

import asyncio

import httpx
import pytest

from obsidian_self_mcp.client import ObsidianVaultClient
from obsidian_self_mcp.config import Config
from obsidian_self_mcp.models import NoteContent


def _doc(path, children, rev="1-a", **extra):
    return {"_id": path.lower(), "path": path, "type": "plain",
            "children": list(children), "_rev": rev, **extra}


class FakeVault(ObsidianVaultClient):
    """In-memory listing and chunk store; records every chunk id fetched."""

    def __init__(self, docs, chunks):
        super().__init__(Config(couch_url="http://unused", db_name="fake-test-db"))
        self.docs = docs
        self.chunks = chunks
        self.fetched: list[str] = []
        self.fail_fetch = False

    async def _get_all_file_docs(self, *, include_deleted=False):
        return [dict(d) for d in self.docs]

    async def _fetch_chunks(self, chunk_ids):
        if self.fail_fetch:
            raise httpx.ConnectError("couchdb unreachable")
        self.fetched.extend(chunk_ids)
        return {c: self.chunks[c] for c in chunk_ids if c in self.chunks}


def _vault():
    docs = [
        _doc("A.md", ["h:a1", "h:a2"]),
        _doc("B.canvas", ["h:b1"]),
        _doc("Plugin.js", ["h:js"]),
        _doc("Unrelated.md", ["h:u1"]),
    ]
    chunks = {
        "h:a1": "Plain [[Target]] and ", "h:a2": "alias [[Folder/Target|t]].",
        "h:b1": '{"text":"see [[Target#Section]]"}',
        "h:js": "var x = '[[Target]]';",
        "h:u1": "links to [[Other]] only",
    }
    return FakeVault(docs, chunks)


def run(coro):
    return asyncio.run(coro)


def test_finds_md_and_canvas_hits_and_ignores_non_link_bearing_files():
    v = _vault()
    hits, failures = run(v.get_backlinks_report("Folder/Target.md"))
    assert sorted(h.source_path for h in hits) == ["A.md", "B.canvas"]
    assert failures == []
    assert "h:js" not in v.fetched


def test_link_split_across_chunk_boundary_is_found():
    v = FakeVault([_doc("S.md", ["h:s1", "h:s2"])], {"h:s1": "x [[Tar", "h:s2": "get]] y"})
    hits, _ = run(v.get_backlinks_report("Target.md"))
    assert [h.source_path for h in hits] == ["S.md"]


def test_note_with_missing_chunk_is_reported_failed_not_read_from_partial_content():
    v = _vault()
    # The chunk that exists still holds the link: a gap-tolerant reader would
    # report this as a hit and call the note fine. It must be a failure.
    v.docs.append(_doc("Damaged.md", ["h:d1", "h:gone"]))
    v.chunks["h:d1"] = "has [[Target]] here"
    hits, failures = run(v.get_backlinks_report("Target.md"))
    assert "Damaged.md" not in [h.source_path for h in hits]
    assert [(f.source_path, f.reason) for f in failures] == [
        ("Damaged.md", "1 of 2 chunks missing from CouchDB")
    ]
    assert "damaged.md" not in v._link_cache


def test_failed_note_is_rescanned_on_every_call():
    v = _vault()
    v.docs.append(_doc("Damaged.md", ["h:gone"]))
    run(v.get_backlinks_report("Target.md"))
    v.fetched.clear()
    _, failures = run(v.get_backlinks_report("Target.md"))
    # Only the damaged note is rescanned; the rest are the two hits' context reads.
    assert sorted(v.fetched) == ["h:a1", "h:a2", "h:b1", "h:gone"]
    assert [f.source_path for f in failures] == ["Damaged.md"]


def test_warm_call_reads_only_hits_for_context():
    v = _vault()
    run(v.get_backlinks_report("Target.md"))
    v.fetched.clear()
    hits, _ = run(v.get_backlinks_report("Target.md"))
    assert len(hits) == 2
    assert sorted(v.fetched) == ["h:a1", "h:a2", "h:b1"]  # context re-read only


def test_changed_children_invalidate_cache():
    v = _vault()
    run(v.get_backlinks_report("Target.md"))
    v.docs[3] = _doc("Unrelated.md", ["h:u2"], rev="2-b")
    v.chunks["h:u2"] = "now links [[Target]]"
    hits, _ = run(v.get_backlinks_report("Target.md"))
    assert "Unrelated.md" in [h.source_path for h in hits]


def test_changed_rev_with_same_children_invalidates_cache():
    v = _vault()
    run(v.get_backlinks_report("Target.md"))
    # A chunk rewritten in place keeps the same id: only _rev can reveal it.
    v.chunks["h:u1"] = "now links [[Target]]"
    v.docs[3] = _doc("Unrelated.md", ["h:u1"], rev="2-b")
    hits, _ = run(v.get_backlinks_report("Target.md"))
    assert "Unrelated.md" in [h.source_path for h in hits]


def test_removed_doc_is_evicted_from_cache():
    v = _vault()
    run(v.get_backlinks_report("Target.md"))
    assert "a.md" in v._link_cache
    v.docs = [d for d in v.docs if d["path"] != "A.md"]
    hits, _ = run(v.get_backlinks_report("Target.md"))
    assert "a.md" not in v._link_cache
    assert [h.source_path for h in hits] == ["B.canvas"]


def test_transport_failure_raises_and_leaves_cache_untouched():
    v = _vault()
    v.fail_fetch = True
    with pytest.raises(httpx.ConnectError):
        run(v.get_backlinks_report("Target.md"))
    assert v._link_cache == {}


def test_uninterpretable_listing_raises_instead_of_reporting_no_backlinks():
    # e.g. LiveSync path obfuscation: every path is ciphertext.
    v = FakeVault([_doc("9f8e7d6c5b4a", ["h:x"])], {"h:x": "[[Target]]"})
    with pytest.raises(RuntimeError, match="refusing to report no backlinks"):
        run(v.get_backlinks_report("Target.md"))


def test_empty_vault_is_a_genuine_empty_result():
    v = FakeVault([], {})
    assert run(v.get_backlinks_report("Target.md")) == ([], [])


class FakeRenameVault(FakeVault):
    """Adds just enough of the note read/write surface to drive rename_note."""

    def __init__(self, docs, chunks, damaged_on_strict=()):
        super().__init__(docs, chunks)
        self.damaged_on_strict = set(damaged_on_strict)
        self.writes: dict[str, str] = {}
        self.tombstoned: list[str] = []
        self.strict_reads: list[str] = []

    async def _get_doc(self, vault_path, *args, **kwargs):
        return next((d for d in self.docs if d["path"] == vault_path), None)

    async def read_note(self, path, strict=False, *, include_deleted=False):
        if strict:
            self.strict_reads.append(path)
            if path in self.damaged_on_strict:
                raise ValueError(f"read_note(strict=True) for {path!r}: chunk missing")
        d = await self._get_doc(path)
        if d is None:
            return None
        content = "".join(self.chunks.get(c, "") for c in d["children"])
        return NoteContent(path=path, content=content, size=len(content))

    async def _write_note_raw_put(self, path, content, *args, **kwargs):
        self.writes[path] = content

    async def _soft_delete_entry_doc(self, path, *args, **kwargs):
        self.tombstoned.append(path)
        return True

    async def _notify_livesync(self, *args, **kwargs):
        pass


def _rename_vault(**kw):
    docs = [_doc("Target.md", ["h:t"]), _doc("Src.md", ["h:s"])]
    chunks = {"h:t": "target body", "h:s": "see [[Target]]"}
    return FakeRenameVault(docs, chunks, **kw)


def test_clean_rename_rewrites_and_tombstones():
    v = _rename_vault()
    summary = run(v.rename_note("Target.md", "Renamed.md"))
    assert v.writes["Src.md"] == "see [[Renamed]]"
    assert v.tombstoned == ["Target.md"]
    assert "NOT deleted" not in summary
    assert "Target.md" in v.strict_reads and "Src.md" in v.strict_reads


def test_scan_failure_blocks_tombstone_and_is_named_in_summary():
    v = _rename_vault()
    v.docs.append(_doc("Damaged.md", ["h:gone"]))
    summary = run(v.rename_note("Target.md", "Renamed.md"))
    assert v.tombstoned == []
    assert "could not scan Damaged.md" in summary
    assert "NOT deleted" in summary


def test_strict_read_failure_on_a_source_blocks_write_back_and_tombstone():
    v = _rename_vault(damaged_on_strict={"Src.md"})
    summary = run(v.rename_note("Target.md", "Renamed.md"))
    assert "Src.md" not in v.writes
    assert v.tombstoned == []
    assert "failed Src.md" in summary


def test_damaged_source_note_stops_rename_before_any_write():
    v = _rename_vault(damaged_on_strict={"Target.md"})
    with pytest.raises(ValueError):
        run(v.rename_note("Target.md", "Renamed.md"))
    assert v.writes == {} and v.tombstoned == []
