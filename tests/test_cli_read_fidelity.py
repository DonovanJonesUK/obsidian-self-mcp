"""`obsidian read` must emit the note's bytes and nothing else.

Regression guard for VHK-DEC-133. `print(note.content)` appended a newline, and notes conventionally
already end with one, so stdout carried one byte the note did not. Reading never mutated anything,
but a tool that captured stdout and wrote it back grew the note by a byte per cycle: verified
9 -> 10 -> 11 -> 12 bytes over three round trips.

These tests drive `_cmd_read` with a stub client so they need no database.
"""

import asyncio
import io
import sys
from contextlib import redirect_stdout
from types import SimpleNamespace

from obsidian_self_mcp.cli import _cmd_read


class _StubClient:
    def __init__(self, content: str, is_binary: bool = False):
        self._note = SimpleNamespace(content=content, is_binary=is_binary, size=len(content))

    async def read_note(self, path, strict=False, include_deleted=False):
        return self._note


def _read_output(content: str) -> str:
    buf = io.StringIO()
    args = SimpleNamespace(path="any.md", strict=False, include_deleted=False)
    with redirect_stdout(buf):
        asyncio.run(_cmd_read(_StubClient(content), args))
    return buf.getvalue()


def test_trailing_newline_is_not_duplicated():
    assert _read_output("line one\n") == "line one\n"


def test_content_without_trailing_newline_gains_none():
    assert _read_output("no trailing newline") == "no trailing newline"


def test_multiline_content_is_byte_exact():
    src = "---\ntype: WorkNote\n---\n\n# Heading\n\nBody.\n"
    assert _read_output(src) == src


def test_multiple_trailing_newlines_are_preserved_exactly():
    """The command reports what is stored; it does not tidy it."""
    assert _read_output("body\n\n\n") == "body\n\n\n"


def test_empty_note_emits_nothing():
    assert _read_output("") == ""


def test_round_trip_is_stable_over_repeated_reads():
    """The accumulation this fixes: feeding output back in must reach a fixed point immediately."""
    content = "line one\n"
    for _ in range(3):
        content = _read_output(content)
    assert content == "line one\n"
