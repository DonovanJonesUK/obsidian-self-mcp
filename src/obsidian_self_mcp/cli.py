"""CLI for Obsidian vault operations via CouchDB."""

import argparse
import asyncio
import sys

from .client import ObsidianVaultClient
from .config import Config


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


async def _cmd_count(client: ObsidianVaultClient, args):
    total = await client.count_notes(folder=args.folder)
    scope = f' under "{args.folder}"' if args.folder else ""
    print(f"{total} notes{scope} (exact, non-paginated)")


async def _cmd_list(client: ObsidianVaultClient, args):
    # Always report the true total (2026-07-30) — the old version printed
    # len(notes) with no indication that the default limit=50 might have
    # silently hidden the rest. That exact gap caused a real production
    # mistake (an audit trusted this command's bare output as complete,
    # missed 22 real files, created duplicates before an independent
    # CouchDB check caught it). Truncation is now impossible to miss.
    total = await client.count_notes(folder=args.folder)
    if getattr(args, "all", False):
        notes = await client.list_notes_all(folder=args.folder)
    else:
        notes = await client.list_notes(folder=args.folder, limit=args.n)
    if not notes:
        print("No notes found.")
        return
    for n in notes:
        print(f"  {n.path}  ({n.size}B, {n.chunk_count} chunks)")
    if len(notes) < total:
        print(f"\n⚠️  Showing {len(notes)} of {total} — TRUNCATED. Use -n {total} or the `count`/`--all` path for the real total.")
    else:
        print(f"\n{len(notes)} notes (complete — this is all of them)")


async def _cmd_read(client: ObsidianVaultClient, args):
    try:
        note = await client.read_note(args.path, strict=getattr(args, "strict", False))
    except ValueError as e:
        print(f"Strict read failed: {e}", file=sys.stderr)
        sys.exit(1)
    if not note:
        print(f"Not found: {args.path}", file=sys.stderr)
        sys.exit(1)
    if note.is_binary:
        print(f"[Binary file, {note.size} bytes]", file=sys.stderr)
    else:
        print(note.content)


async def _cmd_write(client: ObsidianVaultClient, args):
    if args.file:
        with open(args.file) as f:
            content = f.read()
    elif args.content:
        content = args.content
    else:
        content = sys.stdin.read()
    await client.write_note(args.path, content)
    print(f"Written: {args.path} ({len(content.encode('utf-8'))} bytes)")


async def _cmd_search(client: ObsidianVaultClient, args):
    results = await client.search_notes(
        query=args.query, folder=args.d, limit=args.n
    )
    if not results:
        print(f"No results for: {args.query}")
        return
    for r in results:
        print(f"\n{r.path} ({r.matches} matches)")
        for s in r.snippets:
            print(f"  > {s}")


async def _cmd_append(client: ObsidianVaultClient, args):
    if args.file:
        with open(args.file) as f:
            content = f.read()
    elif args.content:
        content = args.content
    else:
        content = sys.stdin.read()
    await client.append_note(args.path, content)
    print(f"Appended to: {args.path}")


async def _cmd_delete(client: ObsidianVaultClient, args):
    if not args.y:
        confirm = input(f"Delete '{args.path}'? [y/N] ")
        if confirm.lower() != "y":
            print("Cancelled.")
            return
    await client.delete_note(args.path)
    print(f"Deleted: {args.path}")


async def _cmd_props(client: ObsidianVaultClient, args):
    if args.set:
        properties = {}
        for pair in args.set:
            if "=" not in pair:
                print(f"Invalid format (use key=value): {pair}", file=sys.stderr)
                sys.exit(1)
            k, v = pair.split("=", 1)
            # Try to parse as JSON for lists/bools/numbers, fall back to string
            import json
            try:
                v = json.loads(v)
            except json.JSONDecodeError:
                pass
            properties[k.strip()] = v
        await client.update_frontmatter(args.path, properties)
        print(f"Updated frontmatter for: {args.path}")
    else:
        fm = await client.read_frontmatter(args.path)
        if fm is None:
            print(f"No frontmatter in: {args.path}")
            return
        for k, v in fm.items():
            print(f"  {k}: {v}")


async def _cmd_tags(client: ObsidianVaultClient, args):
    if args.find:
        notes = await client.search_by_tag(
            tag=args.find, folder=args.folder, limit=args.n
        )
        if not notes:
            print(f"No notes with tag: #{args.find}")
            return
        for n in notes:
            print(f"  {n.path}")
        print(f"\n{len(notes)} notes")
    else:
        tags = await client.list_tags(folder=args.folder)
        if not tags:
            print("No tags found.")
            return
        for tag, count in tags.items():
            print(f"  #{tag}  ({count})")
        print(f"\n{len(tags)} tags")


async def _cmd_backlinks(client: ObsidianVaultClient, args):
    backlinks = await client.get_backlinks(args.path)
    if not backlinks:
        print(f"No backlinks for: {args.path}")
        return
    for bl in backlinks:
        ctx = f"  > {bl.context}" if bl.context else ""
        print(f"  {bl.source_path}")
        if ctx:
            print(ctx)
    print(f"\n{len(backlinks)} backlinks")


async def _cmd_links(client: ObsidianVaultClient, args):
    links = await client.get_outbound_links(args.path)
    if not links:
        print(f"No outbound links in: {args.path}")
        return
    for link in links:
        print(f"  [[{link}]]")
    print(f"\n{len(links)} links")


async def _cmd_rename(client: ObsidianVaultClient, args):
    if not args.y:
        confirm = input(f"Rename '{args.old_path}' → '{args.new_path}'? [y/N] ")
        if confirm.lower() != "y":
            print("Cancelled.")
            return
    result = await client.rename_note(args.old_path, args.new_path)
    print(result)


async def _cmd_folders(client: ObsidianVaultClient, args):
    folders = await client.list_folders()
    if not folders:
        print("No folders found.")
        return
    for f in folders:
        print(f"  {f.path}/  ({f.note_count} notes)")
    print(f"\n{len(folders)} folders")


def main():
    parser = argparse.ArgumentParser(
        prog="obsidian",
        description="Obsidian vault CLI via CouchDB LiveSync",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # list / ls
    p_list = sub.add_parser("list", aliases=["ls"], help="List notes")
    p_list.add_argument("folder", nargs="?", help="Folder to filter")
    p_list.add_argument("-n", type=int, default=50, help="Limit (default 50) — for browsing only, see `count` for the true total")
    p_list.add_argument("--all", action="store_true", help="No limit — the real, complete list (cheap: already fetched in full internally either way)")

    # count — the safe replacement for "does the list output look complete"
    p_count = sub.add_parser("count", help="Exact, non-paginated count of notes (optionally folder-filtered)")
    p_count.add_argument("folder", nargs="?", help="Folder to filter")

    # read / cat
    p_read = sub.add_parser("read", aliases=["cat"], help="Read a note")
    p_read.add_argument("path", help="Vault path to the note")
    p_read.add_argument(
        "--strict", action="store_true",
        help="Raise instead of silently reassembling a gap if a chunk is missing (see read_note(strict=))",
    )

    # write
    p_write = sub.add_parser("write", help="Create/update a note")
    p_write.add_argument("path", help="Vault path")
    p_write.add_argument("content", nargs="?", help="Content (or use -f/stdin)")
    p_write.add_argument("-f", "--file", help="Read content from file")

    # search / grep
    p_search = sub.add_parser("search", aliases=["grep"], help="Search notes")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("-d", help="Folder to search within")
    p_search.add_argument("-n", type=int, default=20, help="Limit (default 20)")

    # append
    p_append = sub.add_parser("append", help="Append to a note")
    p_append.add_argument("path", help="Vault path")
    p_append.add_argument("content", nargs="?", help="Content (or use -f/stdin)")
    p_append.add_argument("-f", "--file", help="Read content from file")

    # delete / rm
    p_delete = sub.add_parser("delete", aliases=["rm"], help="Delete a note")
    p_delete.add_argument("path", help="Vault path")
    p_delete.add_argument("-y", action="store_true", help="Skip confirmation")

    # props
    p_props = sub.add_parser("props", help="Read/set frontmatter properties")
    p_props.add_argument("path", help="Vault path to the note")
    p_props.add_argument("--set", nargs="+", metavar="KEY=VALUE", help="Set properties")

    # tags
    p_tags = sub.add_parser("tags", help="List tags or find notes by tag")
    p_tags.add_argument("folder", nargs="?", help="Folder to filter")
    p_tags.add_argument("--find", metavar="TAG", help="Find notes with this tag")
    p_tags.add_argument("-n", type=int, default=20, help="Limit (default 20)")

    # backlinks
    p_backlinks = sub.add_parser("backlinks", help="Find notes linking to this note")
    p_backlinks.add_argument("path", help="Vault path to the target note")

    # links
    p_links = sub.add_parser("links", help="Show outbound wikilinks from a note")
    p_links.add_argument("path", help="Vault path to the note")

    # rename / mv
    p_rename = sub.add_parser(
        "rename", aliases=["mv"],
        help="Rename a note and propagate wikilink backlinks",
    )
    p_rename.add_argument("old_path", help="Current vault path")
    p_rename.add_argument("new_path", help="New vault path (must not exist)")
    p_rename.add_argument("-y", action="store_true", help="Skip confirmation")

    # folders / tree
    sub.add_parser("folders", aliases=["tree"], help="List folders")

    args = parser.parse_args()

    cmd_map = {
        "list": _cmd_list, "ls": _cmd_list,
        "count": _cmd_count,
        "read": _cmd_read, "cat": _cmd_read,
        "write": _cmd_write,
        "search": _cmd_search, "grep": _cmd_search,
        "append": _cmd_append,
        "delete": _cmd_delete, "rm": _cmd_delete,
        "props": _cmd_props,
        "tags": _cmd_tags,
        "backlinks": _cmd_backlinks,
        "links": _cmd_links,
        "rename": _cmd_rename, "mv": _cmd_rename,
        "folders": _cmd_folders, "tree": _cmd_folders,
    }

    handler = cmd_map[args.command]
    client = ObsidianVaultClient(Config())

    async def run():
        try:
            await handler(client, args)
        finally:
            await client.close()

    _run(run())


if __name__ == "__main__":
    main()
