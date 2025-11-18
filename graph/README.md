# Fabric Graph GQL Example Datasets

This repository contains example datasets that can be used as companions to the [documentation for graph in Microsoft Fabric](https://learn.microsoft.com/fabric/graph).

## Datasets

- `adventureworks_docs_sample`: AdventureWorks dataset
- `ldbc_snb_docs_sample`: Social network dataset

## Files

For each dataset, the following files are provided:

- `zip`: The tables in delta parquet format (in the root folder of the zip file)
- `gty`: The GQL graph type for the data set (optional)
- `md`: Additional remarks (optional)

## How to Use

1. Download relevant files from this repository.

2. In Fabric Lakehouse UI:

   - Upload the zip file for the dataset you need.
   - Extract the contents into the `Tables` section.

3. The Lakehouse will recognise Delta format and register tables automatically.

## Notes

- These datasets are for demo and learning purposes only.


