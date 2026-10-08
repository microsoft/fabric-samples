# TPCH benchmark setup guide for Fabric Warehouse

This guide provides instructions for setting up querying part of TPC-H performance benchmarks using Microsoft Fabric Warehouse, aiming to achieve the best possible performance. While it does not cover the whole benchmarks, it covers querying part of the benchmarks and can be used as a sample workload.

These workload are derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results, as these implementations do not comply with all requirements of the TPC-H Benchmark. For official benchmark guidelines, please refer to [TPC-Homepage](https://www.tpc.org/).

Key TPC-H characteristics:

| **Aspect**          | **TPC-H**                                                    |
| ------------------- | ------------------------------------------------------------ |
| **Purpose**         | Measures  decision support system performance in a data warehouse context. |
| **Data Model**      | Retail  product supplier schema, broadly applicable.         |
| **Schema**          | 8  tables                                                    |
| **Queries**         | Set of  22 ad-hoc queries simulating business intelligence workloads. |
| **Characteristics** | ·      OLAP workload  ·      Large data volumes  ·      High CPU and I/O demands |

To maximize performance in these performance benchmarks, it's crucial to adhere closely to the guidelines outlined in this document.

This guide provides:

- Object creation guidelines and scripts

- Data in parquet format

- Ingestion scripts

- Post-ingestion conditioning script

- TPC-H queries

This guide does not provide:

- Guidance on creating TPC-H dataset. Provided scripts ingest data that is already generated for you.
- Guidance on running non-querying parts of benchmark.
- Tool to execute queries. You can use tool of your choice that works with Fabric Warehouse.

Useful links:

- [Enable Microsoft Fabric for your organization - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch)
- Start [Fabric trial - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/get-started/fabric-trial)

- [Create a workspace - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/get-started/create-workspaces)

- [Create a Warehouse - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-warehouse)

- [Find your Fabric home region - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/admin/find-fabric-home-region)

- Detailed list of [Warehouse performance guidelines - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)

## Prerequisites

Before continuing, please make sure you have:

- Familiarity with TPC-H benchmarks.

- Access to Fabric workspace; useful links: [Getting Started | Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric/getting-started) and [Create a workspace - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/get-started/create-workspaces)

- 1 empty Warehouse; useful link: [Create a Warehouse - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-warehouse)

## Step 1 - Create database objects

This step creates necessary tables in the warehouse, in a way to get optimal performance. Complete list of tables, columns and data types can be found [here](./data-types.md).

**Preparation**: Ensure that you have already set up a data warehouse. This is a prerequisite for the steps that follow.

**Script Execution**: Execute "1-Create objects" script from the appropriate setup folder in [tpch](./tpch/).

**Best practices** (already applied for you in the provided script)**:** The crucial point is to [choose the best data type for performance](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance#choose-the-best-data-type-for-performance):

- **String Data Types**: Choose string data types that are sufficiently long to hold the required values without excess. Avoid overly large string lengths to enhance performance. 
- **Integer Data Types**: Prefer **int** data types over **bigint** wherever feasible. This choice can significantly improve performance, as **int** is generally more performance-friendly in most scenarios.
- **Decimal Data Types**: Opt for decimals with a smaller precision and scale than the default settings. Adjusting these parameters to the minimum necessary can lead to more efficient data processing and storage.
- **Nullability**: Prefer NOT NULL over NULL wherever possible.
- Make sure join columns are of the same type.

By following these steps, you'll be well-positioned to achieve optimal performance in your benchmark testing. Remember, the key is in the details, especially when selecting the most suitable data types for your specific data needs.

## Step 2 - Ingest the data

This step involves importing data into your warehouse. When you use Fabric for data ingestion, the data will be formatted in a way that is optimized for performance. All ingestion into warehouse options are described in [data ingestion guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance#data-ingestion-guidelines).

Below, you will find the specific script for the warehouse to facilitate this data ingestion.

**Preparation**: N/A - scripts that ingest the data from the public storage account are provided in this repo.

**Script Execution**: Execute "2-Ingest data" from the appropriate setup folder in [tpch](./tpch/).

**Best practices** (already applied for you):

- **Number of source files**: For large files, consider splitting your file into multiple files. Delta or parquet format would be preferred over csv due to compression.
- **Source file sizes**: Files should be at least 4 MB in size.
- **Collocation**: For optimal performance, ensure that your source data is in the same geographical region as your warehouse. Storage account used in this repo is in West US 2 region.

Please note that duration of this step may vary depending whether your warehouse is in the same region as source storage account. 

## Step 3 - Post-ingestion conditioning

This step guarantees that the query optimizer has access to the most comprehensive statistics during the query optimization phase. Typically, statistics are automatically generated as needed, drawing from a subset of column values within the table. However, the statistics produced using this script will encompass all values, thereby equipping the query optimizer with the necessary data to determine the most efficient plan.

**Script Execution**: Execute ["3-Post ingestion conditioning"] from the appropriate setup folder in [tpch](./tpch/).

## Step 4 - Benchmark execution

Feel free to use any benchmarking tool compatible with Fabric. However, it's important to [collocate client applications and Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance#collocate-client-applications-and-microsoft-fabric) to ensure that the tool is located in the same geographical region as your warehouse. Adhering to the guidelines outlined in this document will not only help you attain optimal performance in this benchmark but also enhance efficiency in your regular workloads.

You can find scripts in relevant queries folder in [tpch](./tpch/).
