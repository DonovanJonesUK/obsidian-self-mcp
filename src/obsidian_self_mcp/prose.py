"""Prose normalisation applied on the write path.

WHY THIS EXISTS
Obsidian renders with "Strict line breaks" off, so a single newline inside a paragraph becomes a
real line break rather than being collapsed. Prose hard-wrapped to a character ruler therefore
renders as a wall of broken lines. Measured 2026-09-08 across 3,379 vault notes: 87 carried the
signature, 5,577 spurious breaks, and the rate was rising month on month.

The wrapping is not produced by any code in this repo or in PAI's tooling; it arrives already
present in the content handed to `write_note`. There is consequently no upstream component to fix,
which is why normalisation happens here, at the last point before persistence that every text
write passes through: MCP writes, `obsidian write` from the CLI, and headless jobs alike.

See VHK-DEC-133 and `Projects/Vault Housekeeping (VHK)/Vault Prose Line Handling.md`.

SAFETY POSTURE
This module sits in a storage path, so it fails open. If the transform is not idempotent on a given
input, the ORIGINAL content is returned unchanged. A write is never blocked and content is never
left in a state the transform cannot reproduce.

OPT-IN. Normalisation is OFF unless OBSIDIAN_NORMALIZE_PROSE=1 is set in the environment. This
repo is an editable install, so a code change here is immediately live for the production CLI and
for every MCP server process started afterwards; defaulting to off keeps production write
behaviour unchanged until the flag is deliberately set on the units that should have it.
"""

import os
import re

# Lines whose meaning depends on their position. Never joined to, never joined from.
_FENCE = re.compile(r"^\s{0,3}(```|~~~)")
_HEADING = re.compile(r"^\s{0,3}#{1,6}\s")
_TABLE = re.compile(r"^\s*\|")
_QUOTE = re.compile(r"^\s{0,3}>")
_HR = re.compile(r"^\s{0,3}([-*_])(\s*\1){2,}\s*$")
_LIST = re.compile(r"^\s{0,3}([-*+]|\d{1,9}[.)])\s+")
_INDENTED = re.compile(r"^(\s{4,}|\t)")
_FM_DELIM = re.compile(r"^---\s*$")

# A line that is nothing but a link is a complete list entry. Zoottelkeeper writes folder indexes
# as one space-indented wikilink per line, and those lines run 90-100 characters because vault
# paths are long, which is indistinguishable from wrapped prose by length alone. Without this,
# every link in an index is joined onto one line: verified against _Index_of_Lib.md, 39 lines
# collapsed to 10.
_LINK_ONLY = re.compile(r"^\s*(!?\[\[[^\]]*\]\]|\[[^\]]*\]\([^)]*\))\s*$")


def _is_structural(line: str) -> bool:
    return (
        not line.strip()
        or bool(_HEADING.match(line))
        or bool(_TABLE.match(line))
        or bool(_QUOTE.match(line))
        or bool(_HR.match(line))
        or bool(_INDENTED.match(line))
    )


def _is_continuation(line: str) -> bool:
    """Can this line be absorbed as the tail of the paragraph or list item above it?"""
    return (
        not _is_structural(line)
        and not _LIST.match(line)
        and not _FENCE.match(line)
        and not _LINK_ONLY.match(line)
    )


def _unwrap(source: str) -> str:
    lines = source.split("\n")
    out: list[str] = []
    i = 0

    # Frontmatter verbatim, including its closing delimiter. Only when it opens line 1, so a
    # horizontal rule further down the body is never mistaken for a frontmatter block.
    if lines and _FM_DELIM.match(lines[0]):
        out.append(lines[0])
        i = 1
        while i < len(lines) and not _FM_DELIM.match(lines[i]):
            out.append(lines[i])
            i += 1
        if i < len(lines):
            out.append(lines[i])
            i += 1

    in_fence = False
    fence_marker = ""

    while i < len(lines):
        line = lines[i]

        # Fenced code verbatim. The closing fence must match the opener's character, so a ``` inside
        # a ~~~ block does not terminate it early.
        if in_fence:
            out.append(line)
            if _FENCE.match(line) and line.strip().startswith(fence_marker):
                in_fence = False
            i += 1
            continue
        if _FENCE.match(line):
            out.append(line)
            in_fence = True
            fence_marker = line.strip()[:3]
            i += 1
            continue

        if _is_structural(line):
            out.append(line)
            i += 1
            continue

        # A link-only line is a complete entry: neither absorbed nor absorbing. Without the second
        # half, the closing marker of a generated index is pulled onto the final link.
        if _LINK_ONLY.match(line):
            out.append(line)
            i += 1
            continue

        joined = line
        while i + 1 < len(lines) and _is_continuation(lines[i + 1]):
            joined = joined.rstrip() + " " + lines[i + 1].lstrip()
            i += 1
        out.append(joined)
        i += 1

    return "\n".join(out)


def normalize_prose(content: str) -> str:
    """Rejoin hard-wrapped prose. Returns content unchanged if disabled or not idempotent."""
    if os.environ.get("OBSIDIAN_NORMALIZE_PROSE", "0") != "1":
        return content
    if not content:
        return content

    try:
        once = _unwrap(content)
        # Idempotence is what makes this safe to apply on every write: a second pass over stored
        # content must be a no-op, or repeated read-modify-write cycles would drift. If it does not
        # hold for this input, leave the content exactly as the caller sent it.
        if _unwrap(once) != once:
            return content
        return once
    except Exception:
        # A storage path never fails because a formatting nicety raised.
        return content
