INSERT [mtd].[ingest_control] 
								(
									[source_type], 
									[source_container_name], 
									[source_folder_path], 
									[source_database_name], 
									[source_schema_name], 
									[source_table_name], 
									[source_column_list], 
									[source_watermark_column],
									[source_cutoff_time] , 
									[target_object], 
									[load_type], 
									[fabric_store], 
									[enable_flag]
								)

VALUES (
			N'POSTGRESQL', 
			N'Null', 
			N'Null', 
			N'postgres', 
			N'public', 
			N'customer_test', 
			N'Null', 
			N'created_at',
			GETDATE(),
			N'Files/final/raw/', 
			N'Full',
			N'Lakehouse',
			1
		);