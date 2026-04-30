"""Dry run for the widen_tool_id migration.

Reads the database (no writes) and reports exactly what the migration
will touch and what it will NOT touch:

  1. Every column in the database named tool_id (table + length).
  2. Every column on the 3 target tables (so we can prove only the
     tool_id columns are altered).
  3. Row counts on all scheduler_* tables to baseline.
"""
from __future__ import annotations

import os
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        v = v.strip().strip("'").strip('"')
        os.environ.setdefault(k.strip(), v)

from prisma import Prisma  # noqa: E402

TARGETS = [
    "scheduler_yellow_pink_jobs",
    "scheduler_germantown_jobs",
    "scheduler_published_schedule",
]

db = Prisma()
db.connect()

print("=" * 70)
print("1. Every tool_id column in the database")
print("=" * 70)
rows = db.query_raw(
    """SELECT TABLE_SCHEMA, TABLE_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE COLUMN_NAME = 'tool_id'
       ORDER BY TABLE_SCHEMA, TABLE_NAME"""
)
for r in rows:
    target = " <-- WILL ALTER" if r["TABLE_NAME"] in TARGETS else " (untouched)"
    print(f"  {r['TABLE_SCHEMA']}.{r['TABLE_NAME']:35s} "
          f"{r['DATA_TYPE']}({r['CHARACTER_MAXIMUM_LENGTH']}) "
          f"null={r['IS_NULLABLE']}{target}")

print()
print("=" * 70)
print("2. Full column list for each target table")
print("   (proves only tool_id is being touched on these tables)")
print("=" * 70)
for t in TARGETS:
    print(f"\n  [{t}]")
    cols = db.query_raw(
        f"""SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{t}'
            ORDER BY ORDINAL_POSITION"""
    )
    for c in cols:
        marker = " <-- ALTER to NVARCHAR(200)" if c["COLUMN_NAME"] == "tool_id" else ""
        ln = c["CHARACTER_MAXIMUM_LENGTH"]
        ln_s = f"({ln})" if ln is not None else ""
        print(f"    {c['COLUMN_NAME']:24s} {c['DATA_TYPE']}{ln_s:8s} "
              f"null={c['IS_NULLABLE']}{marker}")

print()
print("=" * 70)
print("3. Row counts on all scheduler_* tables (baseline)")
print("=" * 70)
sched_tables = db.query_raw(
    """SELECT name FROM sys.tables
       WHERE name LIKE 'scheduler[_]%' AND schema_id = SCHEMA_ID('dbo')
       ORDER BY name"""
)
for r in sched_tables:
    n = r["name"]
    cnt = db.query_raw(f"SELECT COUNT(*) AS n FROM [dbo].[{n}]")[0]["n"]
    print(f"  {n:40s} rows={cnt}")

print()
print("=" * 70)
print("Migration SQL that WILL run:")
print("=" * 70)
for t in TARGETS:
    print(f"  ALTER TABLE [dbo].[{t}] ALTER COLUMN [tool_id] NVARCHAR(200) NULL;")

print()
print("Dry run complete. No changes made.")
db.disconnect()
