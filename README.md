# obsidian-self-mcp

An MCP server and CLI that gives you direct access to your Obsidian vault through CouchDB — the same database that [Obsidian LiveSync](https://github.com/vrtmrz/obsidian-livesync) uses to sync your notes.

No Obsidian app required. Works on headless servers, in CI pipelines, from AI agents, or anywhere you can run Python.

## Differences from upstream

This is a fork of [suhasvemuri/obsidian-self-mcp](https://github.com/suhasvemuri/obsidian-self-mcp), 38 commits ahead and 0 behind, maintained against a production vault since June 2026.

**Eight of those commits fix data-integrity defects inherited from the original release.** If you are running the upstream version or another fork, these are the ones that matter:

- **`delete_note` destroyed chunks belonging to other notes.** Chunk ids are content-addressed, so identical content across notes resolves to one shared chunk. Deleting any note silently truncated every note sharing a chunk with it. On a 13,408-note vault, 32.6% of notes were unsafe to delete. Now a soft delete that leaves `children` untouched. ([upstream issue #3](https://github.com/suhasvemuri/obsidian-self-mcp/issues/3))
- **Soft-deleted notes were returned as live results**, because LiveSync deletes by a body flag rather than a CouchDB tombstone.
- **`list_notes` silently truncated at 50** with no signal, which caused a real duplicate-file incident. Now reports true totals, warns on truncation, and adds `count_notes`.
- **PyYAML wrapped long frontmatter scalars at 80 columns**, which line-based parsers then read as truncated.
- **Lowercase writes created duplicate vault folders**, because `_id` is lowercased but `path` is stored verbatim.
- **Case-only renames were impossible**, and a note's stored casing was frozen at creation.
- **Newer LiveSync `f:` hash-ID documents were not found**, because lookups keyed on `_id` rather than `path`.
- **`obsidian read` appended a newline the note did not contain**, so any read-then-write round trip grew the file by a byte per cycle.

It also adds `rename_note` with wikilink backlink propagation, a delegated write path using a `livesync-commonlib`-backed writer, opt-in prose normalisation, and substantial startup and query-cost work.

**`search_notes` can answer from a full-text index instead of scanning CouchDB.** The original search runs a regex over every chunk document, which CouchDB cannot index, so its cost grows with the whole database. On a vault of about 300,000 CouchDB documents a rare term took around 50 s and failed against the client timeout. With the optional index the same searches take well under a second. See [Search index](#search-index-optional) below.

Full detail, including what is deliberately *not* fixed: [FORK-CHANGES.md](FORK-CHANGES.md).

## How it works

If you use Obsidian LiveSync, your vault is already stored in CouchDB. This tool talks directly to that CouchDB instance — reading, writing, searching, and managing notes using the same document/chunk format that LiveSync uses. Changes sync back to Obsidian automatically.

## Who this is for

- **Self-hosted LiveSync users** who want programmatic vault access
- **Homelab operators** running headless servers with no GUI
- **AI agent builders** who need to give Claude, GPT, or other agents access to an Obsidian vault via MCP
- **Automation pipelines** that read/write notes (changelogs, daily notes, project docs)

## How this differs from Obsidian's official CLI

Obsidian has an [official CLI](https://obsidian.md/blog/introducing-obsidian-cli/) that requires the Obsidian desktop app running locally and a Catalyst license. This project requires neither — just a CouchDB instance with LiveSync data.

| Feature | Official CLI | obsidian-self-mcp |
|---------|-------------|-------------------|
| **Requires Obsidian app** | Yes (must be running) | No |
| **Requires Catalyst license** | Yes ($25+) | No (MIT, free) |
| **Read/write notes** | Yes | Yes |
| **Search** | Yes | Yes (optional full-text index) |
| **Frontmatter/properties** | Yes | Yes |
| **Tags** | Yes | Yes |
| **Backlinks** | Yes (via app index) | Yes (content scanning) |
| **Templates** | Yes | No (planned) |
| **Canvas** | Yes | No |
| **Graph view** | No | No |
| **Works headless/CI** | No | Yes |
| **MCP server** | No | Yes |
| **Transport** | Local REST API | CouchDB (network) |

## Requirements

- Python 3.10+
- A CouchDB instance with Obsidian LiveSync data
- The database name, URL, and credentials

## Installation

```bash
pip install obsidian-self-mcp
```

Or install from source:

```bash
git clone https://github.com/suhasvemuri/obsidian-self-mcp.git
cd obsidian-self-mcp
pip install -e .
```

## Configuration

Set these environment variables:

```bash
export OBSIDIAN_COUCH_URL="http://your-couchdb-host:5984"
export OBSIDIAN_COUCH_USER="your-username"
export OBSIDIAN_COUCH_PASS="your-password"
export OBSIDIAN_COUCH_DB="obsidian-vault"    # required: there is no default
```

## MCP Server Setup

### Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "obsidian-self-mcp": {
      "command": "python",
      "args": ["-m", "obsidian_self_mcp.server"],
      "env": {
        "OBSIDIAN_COUCH_URL": "http://your-couchdb-host:5984",
        "OBSIDIAN_COUCH_USER": "your-username",
        "OBSIDIAN_COUCH_PASS": "your-password",
        "OBSIDIAN_COUCH_DB": "obsidian-vault"
      }
    }
  }
}
```

### Claude Code

Add to your Claude Code settings (`.claude/settings.json` or global):

```json
{
  "mcpServers": {
    "obsidian-self-mcp": {
      "command": "python",
      "args": ["-m", "obsidian_self_mcp.server"],
      "env": {
        "OBSIDIAN_COUCH_URL": "http://your-couchdb-host:5984",
        "OBSIDIAN_COUCH_USER": "your-username",
        "OBSIDIAN_COUCH_PASS": "your-password",
        "OBSIDIAN_COUCH_DB": "obsidian-vault"
      }
    }
  }
}
```

### Available MCP Tools

| Tool | Description |
|------|-------------|
| `list_notes` | List notes with metadata, optionally filtered by folder |
| `read_note` | Read the full content of a note |
| `write_note` | Create or update a note |
| `search_notes` | Search note content (case-insensitive substring), from the search index when one is running |
| `append_note` | Append content to an existing note |
| `delete_note` | Soft-delete a note, LiveSync-style (chunks left intact) |
| `list_folders` | List all folders with note counts |
| `read_frontmatter` | Read frontmatter properties from a note |
| `update_frontmatter` | Set/update frontmatter properties (JSON input) |
| `list_tags` | List all tags in the vault with counts |
| `search_by_tag` | Find notes containing a specific tag |
| `get_backlinks` | Find notes that link to a given note |
| `get_outbound_links` | List wikilinks from a note |
| `rename_note` | Rename a note and update all wikilink backlinks atomically |

## CLI Usage

The `obsidian` command provides the same operations from the terminal:

```bash
# List notes
obsidian list
obsidian list "Dev Projects" -n 10
obsidian ls                              # alias

# Read a note
obsidian read "Notes/todo.md"
obsidian cat "Notes/todo.md"             # alias

# Write a note
obsidian write "Notes/new.md" "# Hello"
obsidian write "Notes/new.md" -f local-file.md
echo "content" | obsidian write "Notes/new.md"

# Search
obsidian search "kubernetes" -d "Dev Projects" -n 5
obsidian grep "kubernetes"               # alias

# Append to a note
obsidian append "Notes/log.md" "New entry"

# Delete a note
obsidian delete "Notes/old.md"
obsidian rm "Notes/old.md" -y            # skip confirmation

# Frontmatter properties
obsidian props "Notes/todo.md"                      # read properties
obsidian props "Notes/todo.md" --set status=done     # set a property
obsidian props "Notes/todo.md" --set 'tags=["a","b"]' status=active

# Tags
obsidian tags                            # list all tags with counts
obsidian tags "Dev Projects"             # tags in a folder
obsidian tags --find "project"           # find notes with a tag

# Backlinks and links
obsidian backlinks "Notes/todo.md"       # notes linking to this note
obsidian links "Notes/todo.md"           # outbound wikilinks from this note

# Rename a note (updates all wikilink backlinks atomically)
obsidian rename "Notes/old-name.md" "Notes/new-name.md"
obsidian mv "Notes/old-name.md" "Notes/new-name.md"     # alias
obsidian rename "Notes/old-name.md" "Notes/new-name.md" -y  # skip confirmation

# List folders
obsidian folders
obsidian tree                            # alias
```

## Search index (optional)

Without it, `search_notes` runs a regex over every chunk document in the database. CouchDB cannot index a regex, so every search reads the whole database, orphaned chunks included, and the cost grows with the vault. For a small vault that is fine. On a large one, rare terms and terms with no matches become the slowest searches of all.

The index is a small follower process that keeps a SQLite file with one row per note: the note's full text, reassembled from its chunks, under an FTS5 trigram index. It follows CouchDB's `_changes` feed, so an edit is searchable within seconds. `search_notes` and `obsidian search` read that file when it exists and is current.

**Measured on one production vault** (11,700 notes, about 320,000 CouchDB documents):

| | Live scan | Index |
|---|---|---|
| Rare term | about 50 s, usually a timeout | 0.01 to 0.3 s |
| Term with no matches | 49.5 s | under 0.01 s |
| Index file | | 181 MB |
| Full build | | about 60 s |
| Follower memory | | about 125 MiB |

**Beyond speed:**

- A term that straddles a chunk boundary is found.
- Notes that share a chunk are each returned.
- A folder filter is exact and applied before the result limit.
- Base64 attachments and plugin bundles no longer match short terms.
- `.base` files are searched as their decoded text.
- `matches` counts occurrences in the note rather than chunks.

### What it guarantees

- **It never writes to CouchDB.** The only file it writes is its own index.
- **Partial notes are not indexed.** A note with a missing or malformed chunk is recorded as not indexed, never indexed from partial text, and any search whose scope includes it lists it.
- **Soft deletes are honoured.** A note LiveSync soft-deletes (`deleted: true` in the document) leaves the index.
- **A stale or mismatched index is never used silently.** It falls back to the live scan when any of these holds:
  - the file is missing;
  - it was built for another database, server or schema;
  - its follower has not reported for more than 5 minutes;
  - CouchDB holds more than 1,000 changes it has not applied yet.

  When it falls back, **the first line of the result says why**.

### Requirements and limits

- **SQLite 3.34 or later**, for the trigram tokenizer. Check with `python -c "import sqlite3; print(sqlite3.sqlite_version)"`.
- **Plain-text vaults only.** Nothing in this project decrypts. A vault using LiveSync's end-to-end encryption or path obfuscation cannot be read by it at all, index or not.
- **Indexed types:** `.md`, `.canvas`, `.base` and `.txt` by default. Set `OBSIDIAN_SEARCH_INDEX_EXTS` to change the list; the follower rebuilds when it does.
- **One follower per database**, running as the same user as the MCP server. Both use `~/.local/state/obsidian-self-mcp/search-<database>.sqlite`, or `$OBSIDIAN_SEARCH_INDEX_DIR` if set.

### Running it

The follower builds the index on first start, then keeps it current:

```bash
python -m obsidian_self_mcp.search_index follow     # long-running; builds first if needed
python -m obsidian_self_mcp.search_index stats      # note counts, size, last sequence
```

A systemd user unit is included at `systemd/obsidian-search-index@.service`, one instance per database name. Before installing it, edit `EnvironmentFile` to point at a file holding your CouchDB credentials, and `WorkingDirectory`, `PYTHONPATH` and the two Python paths to match your checkout. Then:

```bash
cp systemd/obsidian-search-index@.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now obsidian-search-index@<your-database>.service
loginctl enable-linger "$USER"     # so it starts at boot without a login
```

A manual `build` refuses while a follower holds the index, so stop the unit first if you want to rebuild by hand.

### Settings

| Variable | Default | Effect |
|---|---|---|
| `OBSIDIAN_SEARCH_INDEX` | on | `0`, `false`, `off` or `no` forces the live scan |
| `OBSIDIAN_SEARCH_INDEX_EXTS` | `md,canvas,base,txt` | File types indexed |
| `OBSIDIAN_SEARCH_INDEX_STALE_SECONDS` | `300` | Follower silence after which the index is not used |
| `OBSIDIAN_SEARCH_INDEX_MAX_PENDING` | `1000` | Unapplied changes after which the index is not used |
| `OBSIDIAN_SEARCH_INDEX_DIR` | `~/.local/state/obsidian-self-mcp` | Where index files live |

Design notes and the defects found in review are in [FORK-CHANGES.md](FORK-CHANGES.md).

## How LiveSync stores data

LiveSync splits each note into a parent document (metadata + ordered list of chunk IDs) and one or more chunk documents (the actual content). This tool handles all of that transparently: reads reassemble chunks in order, and writes create proper chunk documents. Deletes never touch chunk documents at all, for the reason set out below.

Document IDs are lowercased vault paths. Paths starting with `_` (like `_Changelog/`) get a `/` prefix since CouchDB reserves `_`-prefixed IDs.

**Chunk-sharing safety:** LiveSync uses content-addressed chunk IDs, so two notes with identical content share the same chunk documents. Nothing in this client may delete a chunk document, because there is no cheap way to know another note does not reference it, measured on one real vault, 9.34% of chunks were multi-referenced and 32.6% of notes shared at least one, the worst chunk with 520 other notes.

`delete_note` therefore performs a LiveSync soft delete: it flags the entry document `deleted: true` and bumps `mtime`, exactly as `deleteDBEntryByPath` in livesync-commonlib does, leaving `children` untouched. The note vanishes from every read path, and writing to the path again restores it. Orphaned chunks are left to LiveSync's garbage collector, they are harmless, whereas a note with its body silently truncated is not. There is no hard-delete or purge verb by design; see SAI-OQ-074.

`rename_note` removes the old *entry* document only, for the same reason, while atomically updating all wikilink backlinks in other notes before the old path disappears.

## License

MIT
