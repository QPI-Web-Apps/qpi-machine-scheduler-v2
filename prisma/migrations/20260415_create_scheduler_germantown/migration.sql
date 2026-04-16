-- scheduler_germantown_jobs: stores green-ticket jobs with "Everything at STF" = N
-- (Germantown work). Populated alongside the other scheduler tables on each
-- "Publish Schedule" click. Uses the processed_indicator (y/n) convention:
-- prior rows flipped to 'n', new rows inserted with 'y'.
--
-- This migration is idempotent and does NOT touch the two other scheduler_* tables
-- or any tables owned by the replenishment / PO acknowledgement projects sharing
-- Portal_QPI. Applied manually via sqlcmd (same pattern as scheduler_runs) — do
-- NOT run `prisma migrate deploy`, which would try to manage the whole DB.

BEGIN TRY

BEGIN TRAN;

IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'scheduler_germantown_jobs' AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE [dbo].[scheduler_germantown_jobs] (
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
        [processed_indicator] NVARCHAR(1)    NOT NULL CONSTRAINT [scheduler_germantown_jobs_processed_indicator_df] DEFAULT 'n',
        CONSTRAINT [scheduler_germantown_jobs_pkey] PRIMARY KEY CLUSTERED ([id])
    );

    CREATE NONCLUSTERED INDEX [scheduler_germantown_jobs_processed_indicator_idx]
        ON [dbo].[scheduler_germantown_jobs]([processed_indicator]);

    CREATE NONCLUSTERED INDEX [scheduler_germantown_jobs_published_at_idx]
        ON [dbo].[scheduler_germantown_jobs]([published_at]);
END;

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH
