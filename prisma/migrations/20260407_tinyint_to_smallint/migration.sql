BEGIN TRY

BEGIN TRAN;

-- AlterTable
ALTER TABLE [dbo].[scheduler_published_schedule] ALTER COLUMN [shift] SMALLINT NULL;
ALTER TABLE [dbo].[scheduler_published_schedule] ALTER COLUMN [priority_class] SMALLINT NULL;

COMMIT TRAN;

END TRY
BEGIN CATCH

IF @@TRANCOUNT > 0
BEGIN
    ROLLBACK TRAN;
END;
THROW

END CATCH

