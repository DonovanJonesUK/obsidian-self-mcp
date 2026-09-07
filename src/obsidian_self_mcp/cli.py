"""CLI for Obsidian vault operations via CouchDB."""

import argparse
import asyncio
import sys

# Block rich and click for the duration of the client import only. httpx does
# `try: from ._main import main / except ImportError: pass`, and httpx._main
# pulls in rich and click — dead weight on every invocation of a CLI that is
# pure argparse and touches neither. Poisoning sys.modules makes that inner
# import fail fast; httpx's own try/except guard is what makes it safe, so
# httpx still imports cleanly, just without its unused `main` entrypoint.
#
# The sentinels are REMOVED again immediately afterwards. sys.modules is
# process-global, so leaving them in place would mean that anything importing
# a helper out of this module — today nothing does, and nothing enforces that —
# silently loses rich and click for the whole process, including the MCP
# server, which genuinely needs both via typer and uvicorn. Deleting them
# restores normal import behaviour while keeping the saving, because httpx
# only attempts `._main` once, at its own first import.
_blocked = [m for m in ("rich", "click") if m not in sys.modules]
for _m in _blocked:
    sys.modules[_m] = None

try:
    from .client import ObsidianVaultClient  # noqa: E402
    from .config import Config  # noqa: E402
finally:
    for _m in _blocked:
        if sys.modules.get(_m) is None:
            del sys.modules[_m]


def run_selftest(quiet: bool = False) -> int:
    """Check the import-block invariants inside this very process.

    The block above is the only place this CLI depends on undocumented
    behaviour of a third-party package: httpx catching ImportError around its
    optional `._main` entrypoint. If a future httpx narrows that clause to
    ModuleNotFoundError, or drops it, every invocation of this command dies at
    import time and every tool that shells out to it breaks at once, for a
    reason nobody would connect to a library upgrade weeks earlier.

    This lives in the CLI rather than in a test file because a test only helps
    if something runs it, and nothing did. `obsidian selftest` can be run by
    hand, by a scheduled job, or after any dependency upgrade.

    Returns a process exit code. With quiet=True nothing is printed unless a
    check fails, so a scheduler can run it unconditionally and hear from it
    only when there is something to say.
    """
    checks: list[tuple[str, bool, str]] = []

    checks.append((
        "httpx imported",
        sys.modules.get("httpx") is not None,
        "httpx is not loaded, so the CLI could not have reached CouchDB anyway",
    ))
    checks.append((
        "httpx._main skipped",
        "httpx._main" not in sys.modules,
        "httpx._main was imported, so the block is no longer firing and every "
        "read is paying for rich and click again",
    ))
    for mod in ("rich", "click"):
        checks.append((
            f"{mod} not loaded",
            mod not in sys.modules,
            f"{mod} was pulled into the process despite the block",
        ))

    # Must run last: this deliberately imports the two modules, so it would
    # invalidate the "not loaded" checks above if it ran before them.
    leak_ok, leak_detail = True, ""
    try:
        import click  # noqa: F401
        import rich  # noqa: F401
    except ImportError as exc:
        leak_ok = False
        leak_detail = (
            f"rich/click are not importable after startup ({exc}); the block "
            "leaked into the process and anything else running here has lost them"
        )
    checks.append(("no leak into the process", leak_ok, leak_detail))

    failed = [(name, detail) for name, ok, detail in checks if not ok]

    if failed:
        print("obsidian selftest: FAILED", file=sys.stderr)
        for name, detail in failed:
            print(f"  FAIL {name}: {detail}", file=sys.stderr)
        print(
            "  The import block lives at the top of obsidian_self_mcp/cli.py. "
            "If httpx has changed, remove the block and accept the slower "
            "startup rather than leaving the CLI broken.",
            file=sys.stderr,
        )
        return 1

    if not quiet:
        for name, _ok, _detail in checks:
            print(f"  ok  {name}")
        print(f"obsidian selftest: {len(checks)} checks passed")
    return 0


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
    include_deleted = getattr(args, "include_deleted", False)
    total = await client.count_notes(folder=args.folder, include_deleted=include_deleted)
    if getattr(args, "all", False):
        notes = await client.list_notes_all(folder=args.folder, include_deleted=include_deleted)
    else:
        notes = await client.list_notes(folder=args.folder, limit=args.n, include_deleted=include_deleted)
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
        note = await client.read_note(
            args.path,
            strict=getattr(args, "strict", False),
            include_deleted=getattr(args, "include_deleted", False),
        )
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
        fm = await client.read_frontmatter(
            args.path, include_deleted=getattr(args, "include_deleted", False)
        )
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
    p_list.add_argument("--include-deleted", action="store_true", help="Include LiveSync-tombstoned (deleted:true) documents, normally filtered out (SAI-OQ-065)")

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
    p_read.add_argument("--include-deleted", action="store_true", help="Include LiveSync-tombstoned (deleted:true) documents, normally filtered out (SAI-OQ-065)")

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
    p_props.add_argument("--include-deleted", action="store_true", help="Include LiveSync-tombstoned (deleted:true) documents, normally filtered out (SAI-OQ-065)")

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

    # selftest
    p_selftest = sub.add_parser(
        "selftest",
        help="Check this CLI's own startup assumptions (no vault access, no credentials)",
    )
    p_selftest.add_argument(
        "--quiet",
        action="store_true",
        help="Print nothing unless a check fails — for schedulers",
    )

    args = parser.parse_args()

    # Handled before the client is constructed: selftest needs no credentials
    # and no CouchDB, and must stay runnable when the vault is unreachable.
    if args.command == "selftest":
        raise SystemExit(run_selftest(quiet=args.quiet))

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
