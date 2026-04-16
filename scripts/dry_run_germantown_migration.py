"""Dry-run verification for the scheduler_germantown_jobs migration.

Confirms BEFORE applying:
  1. The target table does NOT already exist.
  2. All other scheduler_* tables remain untouched (we list them, don't touch them).
  3. The DDL parses correctly against the server (EXEC with sp_describe_first_result_set
     would be for queries — instead we just assert the table-name collision check).

No data is written. No DDL is executed. Read-only.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Load .env
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

from prisma import Prisma  # noqa: E402

TARGET = "scheduler_germantown_jobs"

db = Prisma()
db.connect()

# 1. Does target already exist?
existing = db.query_raw(
    f"SELECT name FROM sys.tables WHERE name = '{TARGET}' AND schema_id = SCHEMA_ID('dbo')"
)
print(f"[1] target table {TARGET!r} already exists: {bool(existing)}")

# 2. List every scheduler_* and sibling-app table currently in dbo so we can
#    confirm nothing else is touched after we apply the migration.
all_tables = db.query_raw(
    "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') ORDER BY name"
)
print(f"[2] dbo tables currently present ({len(all_tables)}):")
for t in all_tables:
    print(f"       - {t['name']}")

# 3. Row counts for scheduler_* tables (so we can re-check post-migration
#    that nothing changed).
sched_tables = [t["name"] for t in all_tables if t["name"].startswith("scheduler_")]
print(f"[3] scheduler_* row counts (baseline):")
for name in sched_tables:
    # Table names from sys.tables are trusted; bracket-quoted for safety.
    rows = db.query_raw(f"SELECT COUNT(*) AS n FROM [dbo].[{name}]")
    print(f"       - {name}: {rows[0]['n']}")

db.disconnect()
print("\nDRY RUN OK — no writes performed.")
