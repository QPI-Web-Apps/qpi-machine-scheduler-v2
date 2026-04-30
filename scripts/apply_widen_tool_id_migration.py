"""Widen tool_id column from NVARCHAR(50) to NVARCHAR(200) on the 3
scheduler tables. Verifies before/after column metadata and that sibling
row counts are unchanged.
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
NEW_LEN = 200

db = Prisma()
db.connect()


def col_info(table: str) -> dict:
    rows = db.query_raw(
        f"""SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = 'tool_id'"""
    )
    return rows[0] if rows else {}


def row_count(table: str) -> int:
    return db.query_raw(f"SELECT COUNT(*) AS n FROM [dbo].[{table}]")[0]["n"]


# Baselines
print("=== Pre-migration ===")
before_cols = {}
before_counts = {}
for t in TARGETS:
    before_cols[t] = col_info(t)
    before_counts[t] = row_count(t)
    print(f"  {t}: tool_id={before_cols[t]}  rows={before_counts[t]}")

# Idempotency: if all already at NEW_LEN, skip.
if all(c.get("CHARACTER_MAXIMUM_LENGTH") == NEW_LEN for c in before_cols.values()):
    print(f"\nAll tool_id columns already NVARCHAR({NEW_LEN}) — nothing to do.")
    db.disconnect()
    raise SystemExit(0)

stmts = [
    f"ALTER TABLE [dbo].[{t}] ALTER COLUMN [tool_id] NVARCHAR({NEW_LEN}) NULL"
    for t in TARGETS
]

print("\n=== Applying ===")
for i, sql in enumerate(stmts, 1):
    print(f"  [{i}/{len(stmts)}] {sql}")
    db.execute_raw(sql)

# Verify
print("\n=== Post-migration ===")
ok = True
for t in TARGETS:
    after = col_info(t)
    after_count = row_count(t)
    print(f"  {t}: tool_id={after}  rows={after_count}")
    if after.get("CHARACTER_MAXIMUM_LENGTH") != NEW_LEN:
        print(f"    !! expected length {NEW_LEN}, got {after.get('CHARACTER_MAXIMUM_LENGTH')}")
        ok = False
    if after.get("IS_NULLABLE") != before_cols[t].get("IS_NULLABLE"):
        print(f"    !! nullability changed")
        ok = False
    if after_count != before_counts[t]:
        print(f"    !! row count changed: {before_counts[t]} -> {after_count}")
        ok = False

db.disconnect()
if not ok:
    raise SystemExit("MIGRATION FAILED VERIFICATION")
print("\nMIGRATION COMPLETE.")
