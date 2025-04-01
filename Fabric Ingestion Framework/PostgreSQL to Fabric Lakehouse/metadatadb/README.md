## About Metadata DB

A SQL metastore is needed to store the metadata we use to control our ingestion pipelines, as well as to store the auditing results (including shortcuts and mirroring auditing). At minimum, you will need a control_table and an audit_table in this DB to perform ingestions.

In this folder you can find scripts to set up your metadata db tables. Please refer to ![PostgreSQL folder](../) for exact instructions.

There are two ways to implement a metadata store DB. 
1. Using Azure SQL DB (covered in this repo) 
2. Using Fabric SQL DB (note that this requires Fabric SQL DB to be enabled in the organisation by tenant admins)