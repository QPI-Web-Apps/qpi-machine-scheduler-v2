"""Apply the scheduler_germantown_jobs migration via Prisma's execute_raw.

Splits the migration SQL into individual statements (prisma engine's
execute_raw accepts a single statement per call) and runs them sequentially.
After running, re-verifies that all pre-existing scheduler_* row counts are
unchanged.
"""
from __future__ import annotations

import os
from pathlib import Path

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

# Baseline counts for sibling tables
sibling_tables = ["scheduler_published_schedule", "scheduler_runs", "scheduler_yellow_pink_jobs"]
baseline = {}
for name in sibling_tables:
    rows = db.query_raw(f"SELECT COUNT(*) AS n FROM [dbo].[{name}]")
    baseline[name] = rows[0]["n"]
print(f"Baseline sibling counts: {baseline}")

# Guard: skip if table already exists
existing = db.query_raw(
    f"SELECT name FROM sys.tables WHERE name = '{TARGET}' AND schema_id = SCHEMA_ID('dbo')"
)
if existing:
    print(f"Table {TARGET!r} already exists — nothing to do.")
    db.disconnect()
    raise SystemExit(0)

# Execute the CREATE + two indexes as separate statements. We intentionally
# do NOT wrap in a BEGIN TRAN here because prisma's execute_raw already runs
# each statement in its own autocommit — the IF NOT EXISTS guard covers
# retry safety.
stmts = [
    f"""CREATE TABLE [dbo].[{TARGET}] (
        [id]                  INT            NOT NULL IDENTITY(1,1),
        [published_at]        DATETIME2      NOT NULL,
        [so_number]           NVARCHAR(50)   NOT NULL,
        [finished_item]       NVARCHAR(100)  NULL,
        [description]         NVARCHAR(500)  NULL,
        [customer]            NVARCHAR(255)  NULL,
        [tool_id]             NVARCHAR(50)   NULL,
        [eqp_code]            NVARCHAR(50)   NULL,
        [remaining_qty]       DECIMAL(12,2)  NULL,
        [run_hours]           DECIMAL(8,2)   NULL,
        [headcount]           DECIMAL(5,2)   NULL,
        [due_date]            DATE           NULL,
        [priority_str]        NVARCHAR(50)   NULL,
        [ticket_color]        NVARCHAR(20)   NULL,
        [processed_indicator] NVARCHAR(1)    NOT NULL CONSTRAINT [{TARGET}_processed_indicator_df] DEFAULT 'n',
        CONSTRAINT [{TARGET}_pkey] PRIMARY KEY CLUSTERED ([id])
    )""",
    f"CREATE NONCLUSTERED INDEX [{TARGET}_processed_indicator_idx] ON [dbo].[{TARGET}]([processed_indicator])",
    f"CREATE NONCLUSTERED INDEX [{TARGET}_published_at_idx] ON [dbo].[{TARGET}]([published_at])",
]

for i, sql in enumerate(stmts, 1):
    print(f"Executing statement {i}/{len(stmts)}...")
    db.execute_raw(sql)

# Verify creation
created = db.query_raw(
    f"SELECT name FROM sys.tables WHERE name = '{TARGET}' AND schema_id = SCHEMA_ID('dbo')"
)
print(f"Created {TARGET!r}: {bool(created)}")

# Verify sibling counts unchanged
after = {}
for name in sibling_tables:
    rows = db.query_raw(f"SELECT COUNT(*) AS n FROM [dbo].[{name}]")
    after[name] = rows[0]["n"]
print(f"Post-migration sibling counts: {after}")
assert after == baseline, f"Sibling tables changed! before={baseline} after={after}"
print("Sibling tables unchanged — OK.")

# Verify schema
cols = db.query_raw(
    f"""SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{TARGET}'
        ORDER BY ORDINAL_POSITION"""
)
print(f"New table has {len(cols)} columns:")
for c in cols:
    print(f"   - {c['COLUMN_NAME']:22s} {c['DATA_TYPE']:12s} nullable={c['IS_NULLABLE']}")

db.disconnect()
print("\nMIGRATION COMPLETE.")
