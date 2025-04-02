# Metadata-driven Data Ingestion Framework in Fabric

Building an effective Lakehouse begins with building a strong foundation with ingestion layer. Ingestion refers to the process of collecting, importing, and processing raw data from various sources into the data lake. Data ingestion is fundamental to the success of a data lake as it enables the consolidation, exploration, and processing of diverse and raw data. It lays the foundation for downstream analytics, machine learning, and reporting activities, providing organizations with the flexibility and agility needed to derive meaningful insights from their data. See this [blog](https://blog.fabric.microsoft.com/en-us/blog/demystifying-data-ingestion-in-fabric-fundamental-components-for-ingesting-data-into-a-fabric-lakehouse-using-fabric-data-pipelines?ft=RK%20Iyer:author) post for more details on the framework.

In this folder, we provide a metadata driven ingestion framework that you can leverage to easily manage the ingestion process and audit the results. 


# Key Contributors
Winnie Li - winnieli@microsoft.com

Dharmendra Keshari - dhkeshar@microsoft.com

Gyani Sinha - gyanisinha@microsoft.com

RK Iyer - raiy@microsoft.com

Abhishek Narain - abnarain@microsoft.com

# Implementation

**Ingestion** can be divided in 2 types - 
- One time Ingestion
  One time ingestion/load refers to initial ingestion of historical data to data lake.
- Incremental ingestion
  Post one time ingestion, incremental ingestion that only capture the new data. 

# Metadata-driven Data Ingestion Framework Components
The Metadata-driven Data Ingestion Framework Components comprises of the following components:
![Framework outline](./PostgreSQL%20to%20Fabric%20Lakehouse/images/PGToFabric1.png)

## Components
|**#**|**Component Name**  |**Purpose**  |
|--|--|--|
| 1 |Control Table | Control table is used to control and select an item from the source system to be moved into a data lake.|
| 2 | Data Ingestion| Data Copy component is used to copy the data from source system to data lake. Typically, same format is used for ingesting data from Source to Bronze Layer/Staging layer but we have the flexibility to change the source type during ingestion.|
| 3 | Auditing |Auditing component is used to audit the records ingested into data lake. Auditing includes identifying any errors or issues that may have occurred during the process, as well as reviewing performance metrics to identify areas for improvement|
| 4 | Notification | Notification component is used to notify in case of success or failure events. This is super critical to ensure that the operation team is notified during all critical events.|
| 5 | Config Management  |Config Management component is used for managing the configuration. |
| 6 | Reporting  |This component is used for reporting. |

---
## Ingestion design
## 1. Control table
This table is used to control and select an item to be moved into a datalake. 

|Column Name|Type |  Purpose|
|--|--|--|
|source_type (*) |nvarchar(20) | Type of source (Blob,ADLS Gen2,Database,AWS S3) **Note**- Use any of these values since they will be used in pipelines |
| source_container_name| nvarchar(50) | Source container name in blob/ADLS and S3 bucket |
| source_folder_path | nvarchar(100) | Source container path in blob/ADLS and S3 bucket |
| source_database_name|nvarchar(100) | Source database name. This will remain Null in case of File ingestion.
| source_schema_name|  nvarchar(50) | Source Schema name. This will remain Null in case of File ingestion.
| source_table_name| nvarchar(100) | Source table name. This will remain Null in case of File ingestion.
| source_column_list| nvarchar(1000) | Source column list indicating the selected column in case of PII columns (applicable for both full & Incr). This may also include columns wherein their data types are not supported and needs to be explicitly converted to be supported in the Destination. This will remain Null in case of File ingestion.
| source_watermark_column | nvarchar(100) | Watermark column to be included for incremental load. This will remain Null in case of File ingestion.
| source_cutoff_time| datetime2(7) | This column is used during full load to restrict the initial load till the specified source_cutoff_time. This will remain Null in case of File ingestion.
| target_container_name (*)| nvarchar(100) | Target container name in ADLS|
| target_folder_path (*)| nvarchar(100) | Target container path in ADLS|
| load_type (*)| nvarchar(50) | Load type can be full or incr(incremental) |
| enable_flag (*)| tinyint| This flag is set to 1 by default. If it is set to 0, the data for this row is not migrated. |

Note - (*) indicates mandatory/not null columns.

## 2. Auditing

Auditing can be performed for all the files & data wherein copy is successful and also for copy failures be it user or system failures. This will also help for future delta loads so that these auditing tables can be referred to in the future. Some of the key fields which can be audited are Item name, data read, data written, rows copied, copy duration, load date-time, status flag — success or failure.


|Column Name  | Type | Purpose |
|--|--|--|
| source_type (*)| nvarchar(20) | Type of source (Blob,ADLS Gen2,Database,AWS S3)  |
| event_run_id (*)| nvarchar(50) | Captured from Pipeline run id (ADF System variable) e.g. 36ff2fde-dd85-4b1f-982e-651530df7c90. This will be JobId in case of Databricks.  |
| event_activity_run_id  | nvarchar(50) | Captured from Activity run id (ADF System variable) 36ff2fde-dd85-4b1f-982e-651530df7c90 |
| item_name (*) | nvarchar(150) | Actual Item Captured from source e.g. For files Container/foldername  & for Database it will be Database.schemaname.tablename  |
| data_read (*) | nvarchar(50) | Total amount of data retrieved from source data store, including data copied in this run and data resumed from the last failed run.   |
| data_written (*)| nvarchar(50) | The actual amount of data written/committed to the sink. The size may be different from dataRead size, as it relates to how each data store stores the data. |
| files_read |int| Total number of files copied from source data store, including files copied in this run and files resumed from the last failed run. Note: Incase of database this field will be NA|
| files_written |int| The actual amount of data written/committed to the sink. The size may be different from dataRead size, as it relates to how each data store stores the data.  |
| rows_read |int| The actual amount of data read from the source. |
| rows_written |int| Number of rows copied to sink. This metric does not apply when copying files as-is without parsing them, for  example,when source and sink datasets are binary format type, or other format type with identical settings.  |
| data_consistency_verification |nvarchar(50)| Tell the result whether the data has been verified to be consistent between source and destination store after being copied.  |
| copy_duration (*) |nvarchar(20)| Total amount of duration take to copy the data.  |
| event_start_time  (*)|datetime2(7)| Time when the pipeline started |
| event_end_time (*)|datetime2(7)| Time when the pipeline ended. |
| source_cutoff_time (*)|datetime2(7)| Source_cutoff_time will be the incremental pipeline execution time. Incase of full load it will be same as source_cutoff_time in the control table. This will be updated on each incremental load.|
| load_type (*)| nvarchar(100)| Status either Full or Incr (Incremental). |
| status (*)|nvarchar(20)| Status either success or failure. |
| event_triggered_by (*)|nvarchar(20)| Status either Manual or Scheduled. |
| error_details | nvarchar(1500)| Contain error codes and messages that pinpoint the location and nature of the problem. |

# Supported Ingestion Implementation
The data sources currently available in this repository is:

- [Azure PostgreSQL DB](./PostgreSQL%20to%20Fabric%20Lakehouse/)

Please refer to the folder for more details.


# Supported Target Format
The supported target formats are:
- Lakehouse

<!-- LICENSE -->
## License
Distributed under the MIT License. See `LICENSE.txt` for more information.

## Productionizing
This module/sample/utility is designed to be a starting point for your own production application, but you should do a thorough review of the security and performance before deploying to production. 
<!-- ACKNOWLEDGMENTS -->