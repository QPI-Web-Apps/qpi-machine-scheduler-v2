"""One-time script: create scheduler_runs table via Prisma execute_raw."""

from prisma import Prisma

SQL = """
IF NOT EXISTS (
    SELECT * FROM sys.tables WHERE name = 'scheduler_runs' AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE [dbo].[scheduler_runs] (
        [id]             INT            NOT NULL IDENTITY(1,1),
        [run_id]         NVARCHAR(8)    NOT NULL,
        [created_at]     DATETIME2      NOT NULL,
        [solver_status]  NVARCHAR(20)   NOT NULL,
        [total_jobs]     INT            NOT NULL,
        [makespan_hours] DECIMAL(8,1)   NOT NULL,
        [skipped_count]  INT            NOT NULL DEFAULT 0,
        [crew_movements] INT            NOT NULL DEFAULT 0,
        [config_json]    NVARCHAR(MAX)  NOT NULL,
        [result_json]    NVARCHAR(MAX)  NOT NULL,
        [note]           NVARCHAR(255)  NULL,
        CONSTRAINT [scheduler_runs_pkey] PRIMARY KEY CLUSTERED ([id])
    );

    CREATE UNIQUE NONCLUSTERED INDEX [scheduler_runs_run_id_key]
        ON [dbo].[scheduler_runs]([run_id]);

    CREATE NONCLUSTERED INDEX [scheduler_runs_created_at_idx]
        ON [dbo].[scheduler_runs]([created_at]);
END
"""

def main():
    db = Prisma()
    db.connect()
    try:
        db.execute_raw(SQL)
        print("scheduler_runs table created (or already exists).")
    finally:
        db.disconnect()

if __name__ == "__main__":
    main()
