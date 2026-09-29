"""Block until CouchDB answers, for a unit's ExecStartPre.

CouchDB runs in Docker, which a user unit cannot order itself after, so at boot
a user service can start before the database is listening (the boot race in
VPSO-OQ-068). Waiting here keeps that race out of the service's own restart
count, which the post-reboot check reads as a restart loop.

Any HTTP response counts as up: CouchDB answers 401 by design without
credentials. Exits 0 once it answers, 1 after the timeout.

Usage: python wait_for_couchdb.py [TIMEOUT_SECONDS]   (reads COUCHDB_URL)
"""

import os
import sys
import time
import urllib.error
import urllib.request

url = os.environ.get("OBSIDIAN_COUCH_URL") or os.environ["COUCHDB_URL"]
deadline = time.monotonic() + float(sys.argv[1] if len(sys.argv) > 1 else 180)
while True:
    try:
        urllib.request.urlopen(url, timeout=5)
        sys.exit(0)
    except urllib.error.HTTPError:
        sys.exit(0)
    except OSError:
        if time.monotonic() > deadline:
            print(f"CouchDB at {url} did not answer in time", file=sys.stderr)
            sys.exit(1)
        time.sleep(3)
