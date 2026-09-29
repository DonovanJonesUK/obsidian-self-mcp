# What this fork changes

`DonovanJonesUK/obsidian-self-mcp`, forked from `suhasvemuri/obsidian-self-mcp`.

20 commits between 2026-06-13 and 2026-09-08, against an upstream whose last code push was 2026-02-28. The fork is 20 ahead and 0 behind, so this is not a set of patches waiting to be rebased; it is the maintained line.

Eight of the twenty fix defects inherited from the initial upstream release rather than introduced here. Those are the ones likely to matter to the other forks, and they are marked below.

## Data integrity

These are the ones that were silently corrupting or silently hiding data. Every one of them reported success while doing so, which is what made them expensive to find.

**Deletion destroyed chunks belonging to other notes.** `delete_note` deleted every id in a note's `children` unconditionally. LiveSync chunk ids are content-addressed, so byte-identical content across notes resolves to one shared chunk document, and deleting any note truncated every other note that shared a chunk with it, mid-word, returning success and logging nothing. Measured on a production vault of 13,408 notes: 9,176 of 98,256 chunks (9.34 percent) multi-referenced, 4,369 notes (32.6 percent) unsafe to delete, worst chunk referenced by 521 notes. Now a soft delete: `deleted: true` on the entry document, `children` untouched, which is the same write `livesync-commonlib` performs in `deleteDBEntryByPath` (`EntryManagerImpls.ts`). Read paths filter on the flag. *(Inherited. Reported upstream as issue #3, unanswered, closed 2026-09-08 with the solution.)*

**Deleted notes came back as live results.** LiveSync deletes by setting `deleted: true` in the document body rather than issuing a CouchDB delete, so there is no tombstone and range queries correctly return them. Read paths now filter. A query using `include_docs=false` cannot see the flag at all and needed a different query shape rather than a filter. *(Inherited.)*

**Listing silently truncated.** `list_notes` fetched the complete matching document set and then sliced to `limit=50` with no signal that anything was hidden. This caused a real incident: an audit trusted `obsidian list` on a folder as complete and created 22 unnecessary duplicate files before an independent count caught it. The same failure had been documented 38 days earlier with workaround-only advice. Fixed at the source rather than in the callers: `count_notes` for exact non-paginated totals, `list_notes_all`, a true-total and truncation warning on every listing, and the same signal on the MCP tool. *(Inherited.)*

**Frontmatter scalars were wrapped and then truncated.** PyYAML defaults to `width=80` and wraps long plain scalars across lines, which naive line-parsers then read as truncated. Now `width=float("inf")`. *(Inherited.)*

**Path casing created duplicate folders.** CouchDB `_id` is lowercased by `normalize_doc_id`, but `path` is stored verbatim from the caller, so a lowercase write created a second folder in Obsidian alongside the correct one rather than writing into it. `_canonicalize_path()` now derives correct casing from existing sibling documents before writing. *(Inherited.)*

**Case-only renames were impossible.** Because `_id` is lowercased, a case-only rename computes the same id for both paths. Two fixes: `path` was create-only, so a document's casing was frozen at first creation regardless of what later callers supplied, and the rename path had to handle old and new resolving to the same document. *(Inherited.)*

**Newer LiveSync hash-ID documents were not found.** Upstream assumed the older id format; newer LiveSync uses `f:...` hash ids where the `path` field is the reliable key. Added a path-field fallback in `get_document` and fixed folder filtering to use `path` over `_id`. *(Inherited.)*

**`obsidian read` emitted a byte the note did not contain.** `print(note.content)` appended a newline that notes conventionally already carry. The read never mutated anything, but any tool capturing stdout and writing it back grew the note by one byte per cycle, without bound. Now `sys.stdout.write`, which is cat semantics. *(Inherited, from the first commit in the repository.)*

## New capability

**`rename_note` with wikilink backlink propagation.** Rewrites `[[Name]]`, `[[Name|alias]]`, `[[Name#heading]]` and `[[Folder/Name]]` across every backlinking note, then soft-deletes the old entry document, leaving chunks intact. Exposed as an MCP tool and as `obsidian rename` / `mv`. Known limitation: wikilinks inside fenced code blocks are also replaced. It does not touch markdown-style links or bare prose mentions, which still need a grep after any rename.

**Delegated write path via a node_writer submodule.** Plain-text writes delegate to a `livesync-commonlib`-backed writer rather than this module's own chunk-id generation and raw PUTs, because chunk ids must be genuinely content-addressed to avoid unbounded chunk-document bloat, and raw PUTs bypass LiveSync's replication-safe write path. The submodule points at `DonovanJonesUK/obsidian-vault-cli`, a fork of `fanselau/obsidian-vault-cli`, after an earlier pointer at upstream left a gitlink reachable from nowhere but one filesystem. A later bump preserves `ctime` on note updates.

**Opt-in write-path prose normalisation.** Rejoins hard-wrapped prose so Obsidian, which renders with Strict line breaks off, shows paragraphs rather than broken lines. Off unless `OBSIDIAN_NORMALIZE_PROSE=1`. Fails open. Structural markdown, and any line that is only a link, pass through untouched.

## Startup cost and hygiene

**Whole-database path scans gated.** A probe that scanned every document ran on paths that did not need it; write paths are now exempt and folder listings are scoped server-side instead of scanning the vault.

**`rich`/`click` kept out of the CLI.** `httpx` imports `httpx._main` behind a `try`/`except ImportError`, which pulls in `rich` and `click` costing roughly 160ms per invocation that this argparse-only CLI never uses. The import block poisons `sys.modules` so that inner import fails fast. `obsidian selftest` exists to catch the day a future `httpx` release drops the guard and turns the block into a hard ImportError at startup.

## Resource bounds

**`search_notes` no longer pulls chunk payloads it will not quote.** The Mango query asked for `fields: ["_id", "data"]`, so the response was proportional to what matched rather than to what was asked for. Measured on production 2026-09-10, query `DFS` over 216,185 documents returned a 1.13 GB body: short tokens match inside base64 attachment chunks, and each of those is large. The server grew past the machine's memory and was OOM-killed, three times in an hour, and systemd's default `OOMPolicy=stop` then failed the whole tmux pane scope, so each kill also cost a Claude session. The query now returns IDs only, ranking happens on IDs, and payloads are fetched in a second bounded pass covering at most `limit * 3` chunks from the notes that survive truncation. Same query afterwards: 20 notes, 6.5s, 125 MiB peak. The caller's `limit` also now sizes the Mango, as a chunk budget of `limit * 25` capped at 5000, because Mango's limit counts chunks rather than notes and passing it through literally would collapse recall. `folder` is applied to the parent file documents before the reverse map is built, which is the earliest point a path is known; it cannot go into the selector, because the selector matches `type: leaf` chunks and a chunk carries no path.

**A slow `search_notes` scan now fails with a message instead of a blank.** The Mango `_find` shared the client's 30s timeout, and by late 2026-09 a rare term took 42-46s on production. httpx timeouts stringify to an empty string and FastMCP reports a tool failure as `Error executing tool search_notes: <str(exc)>`, so callers saw nothing after the colon: 63 such failures logged from 2026-07-09. That one POST now carries its own 120s timeout (`SEARCH_FIND_TIMEOUT`), every other request keeps 30s, and a timeout raises `SearchTimeoutError` naming the elapsed seconds and the alternatives: a more distinctive phrase, a `folder` with a higher `limit` (a floor, not a count), or the `_VAULTSEARCH` skill. The CLI prints it to stderr and exits 1. This bounds the symptom; the scan itself is still un-indexed, see below.

**The server caps its own address space.** `RLIMIT_AS` is set at startup, default 1536 MiB against a healthy footprint of roughly 211 MiB, configurable via `OBSIDIAN_MCP_MEMORY_LIMIT_MB` and disabled with `0`. A cgroup `MemoryMax` was the obvious alternative and is worse here: the server is a stdio child of the terminal pane with no unit of its own, and a limit enforced at allocation time raises `MemoryError` so the tool returns an error and the server keeps serving, rather than the kernel killing a process that has already grown too big to kill quietly. Verified by re-running the pre-fix query shape under the cap: `MemoryError` at 1,126 MiB, process alive.

## Not addressed

The fork does not give general round-trip byte fidelity. Shell `$(...)` still strips trailing newlines, `obsidian write --file` takes the file verbatim, and frontmatter writes can still coerce unquoted scalars through PyYAML: `duration: 4:45` becomes `285`, and `yes`/`no`/`on`/`off` and leading zeros go the same way, on a write aimed at a different field.

`update_frontmatter` prepends a second block rather than merging, so a note that already has frontmatter must be read, rebuilt and written whole.

`search_notes` still runs an un-indexed Mango regex, so every search is a full scan of every chunk document and its cost tracks vault size and rarity of the term rather than result count. Measured on production 2026-09-10: a rare phrase took 32s and a term with no matches took 31.6s. CouchDB says so in the response itself, `"No matching index found, create an index to optimize query time."` Base64 attachment chunks under the 256 KB snippet guard also still surface as results with unreadable snippets, so LiveSync plugin documents appear alongside notes.

A temporary 180 s timeout (`SEARCH_SCAN_TIMEOUT_SECONDS`, commit `7076083`) was applied on `main` on 2026-10-01 to keep production searching; it is superseded by `SEARCH_FIND_TIMEOUT` above. Measured on production the same day: about 55 s for any term, matched or not.
