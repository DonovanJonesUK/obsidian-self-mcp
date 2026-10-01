"""Keep every test away from real search index files and settings.

Without this, a test that expects "index file missing" would read a real index
under ~/.local/state if one existed for its database name, and an exported
OBSIDIAN_SEARCH_INDEX* variable in the shell would change what a test checks.
"""

import os

import pytest


@pytest.fixture(autouse=True)
def _isolated_search_index(tmp_path_factory, monkeypatch):
    for name in list(os.environ):
        if name.startswith("OBSIDIAN_SEARCH_INDEX"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("OBSIDIAN_SEARCH_INDEX_DIR", str(tmp_path_factory.mktemp("index-dir")))
