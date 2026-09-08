# obsidian-self-mcp

An MCP server and CLI that gives you direct access to your Obsidian vault through CouchDB — the same database that [Obsidian LiveSync](https://github.com/vrtmrz/obsidian-livesync) uses to sync your notes.

No Obsidian app required. Works on headless servers, in CI pipelines, from AI agents, or anywhere you can run Python.

## Differences from upstream

This is a fork of [suhasvemuri/obsidian-self-mcp](https://github.com/suhasvemuri/obsidian-self-mcp), 20 commits ahead and 0 behind, maintained against a production vault since June 2026.

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
| **Search** | Yes | Yes |
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
export OBSIDIAN_COUCH_DB="obsidian-vault"    # optional, defaults to "obsidian-vault"
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
| `search_notes` | Search note content (case-insensitive) |
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

## How LiveSync stores data

LiveSync splits each note into a parent document (metadata + ordered list of chunk IDs) and one or more chunk documents (the actual content). This tool handles all of that transparently: reads reassemble chunks in order, and writes create proper chunk documents. Deletes never touch chunk documents at all, for the reason set out below.

Document IDs are lowercased vault paths. Paths starting with `_` (like `_Changelog/`) get a `/` prefix since CouchDB reserves `_`-prefixed IDs.

**Chunk-sharing safety:** LiveSync uses content-addressed chunk IDs, so two notes with identical content share the same chunk documents. Nothing in this client may delete a chunk document, because there is no cheap way to know another note does not reference it, measured on one real vault, 9.34% of chunks were multi-referenced and 32.6% of notes shared at least one, the worst chunk with 520 other notes.

`delete_note` therefore performs a LiveSync soft delete: it flags the entry document `deleted: true` and bumps `mtime`, exactly as `deleteDBEntryByPath` in livesync-commonlib does, leaving `children` untouched. The note vanishes from every read path, and writing to the path again restores it. Orphaned chunks are left to LiveSync's garbage collector, they are harmless, whereas a note with its body silently truncated is not. There is no hard-delete or purge verb by design; see SAI-OQ-074.

`rename_note` removes the old *entry* document only, for the same reason, while atomically updating all wikilink backlinks in other notes before the old path disappears.

## License

MIT
