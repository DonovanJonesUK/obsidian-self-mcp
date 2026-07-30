#!/usr/bin/env bash
# Go-live smoke test for obsidian-write-daemon.service (ISA
# 20260728-180000_travel-resilient-vault-sync-architecture, ISC-54,
# VPSO-DEC-044). Run this AFTER `systemctl --user enable --now
# obsidian-write-daemon.service`, before trusting it for real MCP traffic.
#
# Every check is against obsidiandb-dev only. Production is deliberately
# NOT exercised here — not because the DB is unknown (VPSO-OQ-SYNC-005
# resolved 2026-07-30: obsidian-thin0726x2 is production, VPSO-DEC-045),
# but because a real production write is its own explicit go/no-go
# decision, same as every other real-write step in this ISA. Separately,
# VPSO-OQ-SYNC-009 (rwf-tooling gets 403 on every DB) means RWF's own
# write path specifically is still broken regardless of this daemon.
#
# Usage: bash scripts/smoke_test_write_daemon.sh
# Exit 0 = all checks passed. Non-zero = see the FAIL line.

set -u
PASS=0
FAIL=0

step() { echo; echo "── $1 ──"; }
ok()   { echo "  PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

SOCK="${OBSIDIAN_WRITE_DAEMON_SOCK:-${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/obsidian-write-daemon.sock}"
DEV_PASS="$(python3 -c "import json; print(json.load(open('$HOME/.claude.json'))['mcpServers']['obsidian-self-mcp-dev']['env']['OBSIDIAN_COUCH_PASS'])")"
CLI="$HOME/obsidian-self-mcp/.venv/bin/obsidian"
export OBSIDIAN_COUCH_URL=http://localhost:5984
export OBSIDIAN_COUCH_USER=admin
export OBSIDIAN_COUCH_PASS="$DEV_PASS"
export OBSIDIAN_COUCH_DB=obsidiandb-dev
TEST_NOTE="_smoke-test/$(date +%s).md"

step "1. systemd unit state"
if systemctl --user is-enabled obsidian-write-daemon.service >/dev/null 2>&1; then
  ok "unit is enabled"
else
  bad "unit is NOT enabled (systemctl --user is-enabled)"
fi
if systemctl --user is-active obsidian-write-daemon.service >/dev/null 2>&1; then
  ok "unit is active"
else
  bad "unit is NOT active — nothing else in this script will pass"
fi

step "2. socket file exists"
if [ -S "$SOCK" ]; then
  ok "socket present at $SOCK"
else
  bad "no socket at $SOCK"
fi

step "3. stats round-trip (basic IPC liveness)"
STATS=$(python3 - "$SOCK" <<'PYEOF'
import socket, json, sys
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(sys.argv[1])
    s.sendall((json.dumps({"op": "stats"}) + "\n").encode())
    print(s.recv(65536).decode().strip())
except Exception as e:
    print(f"ERROR: {e}")
PYEOF
)
if echo "$STATS" | grep -q '"ok":true'; then
  ok "stats responded: $STATS"
else
  bad "stats did not respond cleanly: $STATS"
fi

step "4. real write + read-back + delete (obsidiandb-dev)"
WRITE_OUT=$("$CLI" write "$TEST_NOTE" "smoke test $(date -Iseconds)" 2>&1)
if echo "$WRITE_OUT" | grep -q "^Written:"; then
  ok "write succeeded: $WRITE_OUT"
  READ_OUT=$("$CLI" read "$TEST_NOTE" 2>&1)
  if echo "$READ_OUT" | grep -q "smoke test"; then
    ok "read-back matched"
  else
    bad "read-back did not match: $READ_OUT"
  fi
  "$CLI" delete -y "$TEST_NOTE" >/dev/null 2>&1
else
  bad "write failed: $WRITE_OUT"
fi

step "5. restart-recovery (kills + confirms client-side retry rides through it)"
systemctl --user restart obsidian-write-daemon.service
# Fire a write immediately — the daemon is very likely still restarting;
# client.py's _delegate_write() should retry once after ~3s and succeed,
# proving the Q3 health/liveness design actually works, not just in theory.
RESTART_NOTE="_smoke-test/restart-$(date +%s).md"
RESTART_START=$(date +%s)
RESTART_OUT=$("$CLI" write "$RESTART_NOTE" "restart recovery test" 2>&1)
RESTART_ELAPSED=$(( $(date +%s) - RESTART_START ))
if echo "$RESTART_OUT" | grep -q "^Written:"; then
  ok "write succeeded across a live restart (took ${RESTART_ELAPSED}s — a value near 3s means the retry path fired, near 0s means the daemon was already back up)"
  "$CLI" delete -y "$RESTART_NOTE" >/dev/null 2>&1
else
  bad "write did NOT survive a live restart: $RESTART_OUT"
fi

step "6. journal error scan (last 100 lines)"
ERR_LINES=$(journalctl --user -u obsidian-write-daemon.service --no-pager -n 100 2>/dev/null | grep -iE "error|fatal|unhandled|uncaught" | grep -v "ExperimentalWarning\|DeprecationWarning")
if [ -z "$ERR_LINES" ]; then
  ok "no error/fatal lines in recent journal"
else
  bad "found error-looking journal lines:"
  echo "$ERR_LINES" | sed 's/^/    /'
fi

step "7. production DB (obsidian-thin0726x2) — DELIBERATELY NOT TESTED"
echo "  SKIPPED: DB identity is known (VPSO-OQ-SYNC-005 resolved), but a real"
echo "  production write is its own explicit go/no-go decision, not implied by"
echo "  running this script. Separately, VPSO-OQ-SYNC-009 (rwf-tooling gets 403"
echo "  on every DB) means RWF's own write path is broken regardless — that's"
echo "  a different credential than this check would use (admin, matching"
echo "  obsidian-self-mcp's own config), so it wouldn't even catch that gap."

echo
echo "════════════════════════════════════"
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && echo "Dev-side smoke test clean. Real production write still requires a separate explicit go-ahead."
echo "════════════════════════════════════"
exit "$FAIL"
