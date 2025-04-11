/******* sample script for inserts into the control table *******/

-- INSERT [mtd].[ingest_control] ([source_type], [source_container_name], [source_folder_path], [source_database_name], [source_schema_name], [source_table_name], [source_column_list], [source_watermark_column],[source_cutoff_time] , [target_object], [load_type], [fabric_store], [enable_flag])
-- VALUES (N'POSTGRESQL', N'Null', N'Null', N'contosodb', N'public', N'customer_test', N'Null', N'created_at',GETDATE(),N'Files/final/raw/', N'Incr',N'Lakehouse',1);

/****** Object:  Table [mtd].[ingest_audit]    Script Date: 14/2/2025 9:10:13 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

USE metadatadb
GO

CREATE SCHEMA mtd
GO

CREATE TABLE [mtd].[ingest_audit](
	[source_type] [varchar](20) NULL,
	[event_run_id] [varchar](50) NULL,
	[event_activity_run_id] [varchar](50) NULL,
	[item_name] [varchar](150) NULL,
	[data_read] [bigint] NULL,
	[data_written] [bigint] NULL,
	[files_read] [int] NULL,
	[files_written] [int] NULL,
	[rows_read] [bigint] NULL,
	[rows_written] [bigint] NULL,
	[data_consistency_verification] [varchar](50) NULL,
	[copy_duration] [int] NULL,
	[event_start_time] [datetime2](6) NULL,
	[event_end_time] [datetime2](6) NULL,
	[source_cutoff_time] [datetime2](6) NULL,
	[load_type] [varchar](100) NULL,
	[status] [varchar](20) NULL,
	[event_triggered_by] [varchar](20) NULL,
	[error_details] [varchar](1500) NULL,
	[pipeline_url] [varchar](500) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [mtd].[ingest_control]    Script Date: 14/2/2025 9:10:14 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [mtd].[ingest_control](
	[control_id] [int] IDENTITY(1,1) NOT NULL,
	[source_type] [varchar](20) NOT NULL,
	[source_container_name] [varchar](50) NULL,
	[source_folder_path] [varchar](100) NULL,
	[source_database_name] [varchar](100) NULL,
	[source_schema_name] [varchar](50) NULL,
	[source_table_name] [varchar](100) NULL,
	[source_column_list] [varchar](1000) NULL,
	[source_watermark_column] [varchar](100) NULL,
	[source_cutoff_time] [datetime2](6) NULL,
	[target_object] [varchar](100) NOT NULL,
	[load_type] [varchar](100) NOT NULL,
	[fabric_store] [varchar](50) NOT NULL,
	[enable_flag] [int] NOT NULL
) ON [PRIMARY]
GO


CREATE PROCEDURE [mtd].[capture_audit_event_sp] 
@source_type VARCHAR(20),
@event_run_id VARCHAR(50),
@event_activity_run_id VARCHAR(50) = NULL,
@item_name VARCHAR(150),
@data_read bigint,
@data_written bigint,
@files_read INT = NULL,
@files_written INT = NULL,
@rows_read BIGINT = NULL,
@rows_written BIGINT = NULL,
@data_consistency_verification VARCHAR(50) = NULL,
@copy_duration integer,
@event_start_time DATETIME2(7),
@event_end_time DATETIME2(7),
@source_cutoff_time DATETIME2(7) = NULL,
@load_type VARCHAR(100),
@status VARCHAR(20),
@event_triggered_by VARCHAR(20),
@error_details VARCHAR(1500) = NULL,
@pipeline_url VARCHAR(500)
AS
BEGIN
SET NOCOUNT ON
INSERT INTO mtd.ingest_audit (
                            source_type,
                            event_run_id,
                            event_activity_run_id,
                            item_name,
                            data_read,
                            data_written,
                            files_read,
                            files_written,
                            rows_read,
                            rows_written,
                            data_consistency_verification,
                            copy_duration,
                            event_start_time,
                            event_end_time,
                            source_cutoff_time,
                            load_type,
                            status,
                            event_triggered_by,
			error_details,
          pipeline_url
                        )
VALUES                      (
                             @source_type
                            , @event_run_id
                            , @event_activity_run_id
                            , @item_name
                            , @data_read
                            , @data_written
                            , @files_read
                            , @files_written
                            , @rows_read
                            , @rows_written
                            , @data_consistency_verification
                            , @copy_duration
                            , @event_start_time
                            , @event_end_time
                            , @source_cutoff_time
                            , @load_type
                            , @status
                            , @event_triggered_by
			                  , @error_details
                            ,@pipeline_url
                          )
END
GO

