"""Guards the rich/click import block at the top of cli.py.

The block exists because httpx/__init__.py imports httpx._main behind a
try/except ImportError, and httpx._main pulls in rich and click (~160ms per
CLI invocation) that this argparse-only CLI never uses. Poisoning sys.modules
makes that inner import fail fast. If a future httpx release drops the
try/except guard, the block turns into a hard ImportError at CLI startup —
test (a) below catches that, loudly.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


def _python() -> str:
    return str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_python(), "-c", code],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_cli_entrypoint_still_imports():
    """The CLI entrypoint imports cleanly with the sys.modules block in place."""
    result = _run("from obsidian_self_mcp.cli import main")
    assert result.returncode == 0, (
        "importing obsidian_self_mcp.cli failed — if this is an ImportError for "
        "rich or click, httpx has dropped its try/except guard around "
        "httpx._main and the block in cli.py must be removed.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_block_still_fires_and_httpx_still_imports():
    """The saving is real: httpx loaded, httpx._main (and so rich/click) not.

    Asserts the invariant rather than the mechanism. `httpx._main` absent from
    sys.modules is the thing that makes the CLI faster; the sys.modules
    sentinels are only how that is achieved, and they are deliberately removed
    again by cli.py, so asserting on them would test the implementation.
    """
    code = (
        "import sys\n"
        "import obsidian_self_mcp.cli\n"
        "assert sys.modules.get('httpx') is not None, 'httpx did not import'\n"
        "assert 'httpx._main' not in sys.modules, "
        "'httpx._main was imported — the block in cli.py is not firing'\n"
        "assert 'rich' not in sys.modules, 'rich was loaded'\n"
        "assert 'click' not in sys.modules, 'click was loaded'\n"
        "print('OK')\n"
    )
    result = _run(code)
    assert result.returncode == 0, (
        f"import hygiene assertions failed.\nstdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
    assert "OK" in result.stdout


def test_block_does_not_leak_into_the_process():
    """Importing cli.py must leave rich and click importable afterwards.

    sys.modules is process-global. If cli.py left its sentinels behind, any
    process that imported a helper out of it — including, one refactor from
    now, the MCP server — would lose rich and click with no error until
    something needed them.
    """
    code = (
        "import obsidian_self_mcp.cli\n"
        "import rich, click\n"
        "assert rich is not None and click is not None\n"
        "print('OK')\n"
    )
    result = _run(code)
    assert result.returncode == 0, (
        "rich/click could not be imported after importing cli.py — the block "
        "leaked into the process.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout


if __name__ == "__main__":
    # Runnable without pytest, deliberately: this venv has no test runner
    # installed and adding one to a production-serving environment is a
    # separate decision. `python tests/test_cli_import_hygiene.py` is the
    # tripwire's real invocation today.
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    raise SystemExit(1 if failures else 0)
