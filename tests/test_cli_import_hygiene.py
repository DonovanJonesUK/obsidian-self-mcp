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
    """rich and click stay blocked, and httpx imports anyway."""
    code = (
        "import sys\n"
        "import obsidian_self_mcp.cli\n"
        "assert 'rich' in sys.modules and sys.modules['rich'] is None, "
        "'rich block missing or overwritten by a real import'\n"
        "assert 'click' in sys.modules and sys.modules['click'] is None, "
        "'click block missing or overwritten by a real import'\n"
        "assert 'httpx' in sys.modules and sys.modules['httpx'] is not None, "
        "'httpx did not import'\n"
        "print('OK')\n"
    )
    result = _run(code)
    assert result.returncode == 0, (
        f"import hygiene assertions failed.\nstdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
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
