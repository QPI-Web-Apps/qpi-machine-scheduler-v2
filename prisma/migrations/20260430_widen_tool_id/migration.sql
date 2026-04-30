BEGIN TRY

BEGIN TRAN;

-- AlterTable: widen tool_id from NVARCHAR(50) to NVARCHAR(200).
-- Changeover entries store "FROM_TOOL -> TO_TOOL" which can exceed 50 chars
-- when tool numbers are long.
ALTER TABLE [dbo].[scheduler_yellow_pink_jobs]   ALTER COLUMN [tool_id] NVARCHAR(200) NULL;
ALTER TABLE [dbo].[scheduler_germantown_jobs]    ALTER COLUMN [tool_id] NVARCHAR(200) NULL;
ALTER TABLE [dbo].[scheduler_published_schedule] ALTER COLUMN [tool_id] NVARCHAR(200) NULL;

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH
