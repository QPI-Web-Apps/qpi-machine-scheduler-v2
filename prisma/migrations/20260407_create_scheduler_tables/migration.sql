BEGIN TRY

BEGIN TRAN;

-- CreateTable
CREATE TABLE [dbo].[scheduler_yellow_pink_jobs] (
    [id] INT NOT NULL IDENTITY(1,1),
    [published_at] DATETIME2 NOT NULL,
    [so_number] NVARCHAR(50) NOT NULL,
    [part_number] NVARCHAR(100),
    [description] NVARCHAR(500),
    [tool_id] NVARCHAR(50),
    [machine] NVARCHAR(20),
    [due_date] DATE,
    [scheduled_start] DATETIME2,
    [ticket_color] NVARCHAR(20) NOT NULL,
    [job_type] NVARCHAR(50),
    [processed_indicator] NVARCHAR(1) NOT NULL CONSTRAINT [scheduler_yellow_pink_jobs_processed_indicator_df] DEFAULT 'n',
    CONSTRAINT [scheduler_yellow_pink_jobs_pkey] PRIMARY KEY CLUSTERED ([id])
);

-- CreateTable
CREATE TABLE [dbo].[scheduler_published_schedule] (
    [id] INT NOT NULL IDENTITY(1,1),
    [published_at] DATETIME2 NOT NULL,
    [machine_id] NVARCHAR(20) NOT NULL,
    [entry_type] NVARCHAR(20) NOT NULL,
    [start_time] DATETIME2 NOT NULL,
    [end_time] DATETIME2 NOT NULL,
    [shift] TINYINT,
    [machine_group] NVARCHAR(50),
    [tool_id] NVARCHAR(50),
    [so_number] NVARCHAR(50),
    [finished_item] NVARCHAR(100),
    [description] NVARCHAR(500),
    [customer] NVARCHAR(255),
    [remaining_qty] DECIMAL(12,2),
    [run_hours] DECIMAL(8,2),
    [headcount] DECIMAL(5,2),
    [due_date] DATE,
    [priority_class] TINYINT,
    [ticket_color] NVARCHAR(20),
    [is_labeler] BIT,
    [is_bagger] BIT,
    [is_in_progress] BIT,
    [is_picked] BIT,
    [crew_from] NVARCHAR(20),
    [crew_to] NVARCHAR(20),
    [idle_type] NVARCHAR(20),
    [processed_indicator] NVARCHAR(1) NOT NULL CONSTRAINT [scheduler_published_schedule_processed_indicator_df] DEFAULT 'n',
    CONSTRAINT [scheduler_published_schedule_pkey] PRIMARY KEY CLUSTERED ([id])
);

-- CreateIndex
CREATE NONCLUSTERED INDEX [scheduler_yellow_pink_jobs_processed_indicator_idx] ON [dbo].[scheduler_yellow_pink_jobs]([processed_indicator]);

-- CreateIndex
CREATE NONCLUSTERED INDEX [scheduler_yellow_pink_jobs_published_at_idx] ON [dbo].[scheduler_yellow_pink_jobs]([published_at]);

-- CreateIndex
CREATE NONCLUSTERED INDEX [scheduler_published_schedule_processed_indicator_idx] ON [dbo].[scheduler_published_schedule]([processed_indicator]);

-- CreateIndex
CREATE NONCLUSTERED INDEX [scheduler_published_schedule_published_at_idx] ON [dbo].[scheduler_published_schedule]([published_at]);

-- CreateIndex
CREATE NONCLUSTERED INDEX [scheduler_published_schedule_machine_id_start_time_idx] ON [dbo].[scheduler_published_schedule]([machine_id], [start_time]);

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH

