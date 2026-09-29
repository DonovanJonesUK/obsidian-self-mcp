"""Tests for the search_notes `_find` timeout.

The defect this guards against is a blank error: httpx timeouts stringify to
"", so a scan that outran the shared 30s client reached MCP callers as
`Error executing tool search_notes: ` with nothing after it. Each case drives
the real search_notes code path through an httpx.MockTransport.
"""

import asyncio

import httpx
import pytest

from obsidian_self_mcp.client import ObsidianVaultClient, SearchTimeoutError
from obsidian_self_mcp.config import Config


def _doc(path, children, rev="1-a", **extra):
    return {"_id": path.lower(), "path": path, "type": "plain",
            "children": list(children), "_rev": rev, **extra}


class FakeVault(ObsidianVaultClient):
    """In-memory listing and chunk store; `_find` goes through a MockTransport."""

    def __init__(self, docs, chunks, find_handler=None):
        super().__init__(Config(couch_url="http://unused", db_name="fake-test-db"))
        self.docs = docs
        self.chunks = chunks
        self.find_requests: list[httpx.Request] = []
        if find_handler is not None:
            def handler(request: httpx.Request) -> httpx.Response:
                if request.method == "POST" and request.url.path.endswith("/_find"):
                    self.find_requests.append(request)
                    return find_handler(request)
                raise AssertionError(f"unexpected request {request.method} {request.url}")
            # _get_client() returns this while it is open; the default
            # timeout here is deliberately the shared client's 30s, so only a
            # per-request override can produce 120s at the transport.
            self._client = httpx.AsyncClient(
                base_url="http://fake/fake-test-db",
                transport=httpx.MockTransport(handler),
                timeout=30.0,
            )

    async def _get_all_file_docs(self, *, include_deleted=False):
        return [dict(d) for d in self.docs]

    async def _fetch_chunks(self, chunk_ids):
        return {c: self.chunks[c] for c in chunk_ids if c in self.chunks}


def _vault(find_handler=None):
    docs = [_doc("Notes/A.md", ["h:a1"]), _doc("Notes/B.md", ["h:b1"])]
    chunks = {"h:a1": "some prose mentioning the rare phrase here",
              "h:b1": "nothing relevant"}
    return FakeVault(docs, chunks, find_handler)


def run(coro):
    return asyncio.run(coro)


def _timing_out(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("", request=request)


def test_find_timeout_raises_non_blank_actionable_error():
    v = _vault(_timing_out)

    async def go():
        try:
            with pytest.raises(SearchTimeoutError) as info:
                await v.search_notes("rare phrase")
            return info.value
        finally:
            await v.close()

    exc = run(go())
    msg = str(exc)
    assert msg.strip() != ""
    assert "timed out after" in msg and "s:" in msg
    assert "whole-vault chunk scan" in msg
    assert "_VAULTSEARCH" in msg
    assert "folder" in msg and "limit" in msg and "floor" in msg
    assert "'rare phrase'" in msg
    assert isinstance(exc.__cause__, httpx.ReadTimeout)
    assert len(v.find_requests) == 1


def test_find_post_carries_120s_per_request_timeout_not_30():
    def ok(request):
        return httpx.Response(200, json={"docs": []})

    v = _vault(ok)

    async def go():
        try:
            return await v.search_notes("rare phrase")
        finally:
            await v.close()

    assert run(go()) == []
    assert len(v.find_requests) == 1
    timeout = v.find_requests[0].extensions["timeout"]
    assert timeout["read"] == ObsidianVaultClient.SEARCH_FIND_TIMEOUT == 120.0
    assert timeout["read"] != 30.0


def test_shared_client_keeps_30s_default():
    v = ObsidianVaultClient(Config(couch_url="http://unused", db_name="fake-test-db"))

    async def go():
        try:
            client = await v._get_client()
            return client.timeout
        finally:
            await v.close()

    t = run(go())
    assert (t.connect, t.read, t.write, t.pool) == (30.0, 30.0, 30.0, 30.0)


def test_fast_find_still_returns_results():
    def ok(request):
        return httpx.Response(200, json={"docs": [{"_id": "h:a1"}]})

    v = _vault(ok)

    async def go():
        try:
            return await v.search_notes("rare phrase")
        finally:
            await v.close()

    results = run(go())
    assert [r.path for r in results] == ["Notes/A.md"]
    assert results[0].matches == 1
    assert len(results[0].snippets) == 1
    assert "rare phrase" in results[0].snippets[0]


def test_non_timeout_errors_are_not_rewrapped():
    def boom(request):
        raise httpx.ConnectError("couchdb unreachable", request=request)

    v = _vault(boom)

    async def go():
        try:
            with pytest.raises(httpx.ConnectError):
                await v.search_notes("rare phrase")
        finally:
            await v.close()

    run(go())
