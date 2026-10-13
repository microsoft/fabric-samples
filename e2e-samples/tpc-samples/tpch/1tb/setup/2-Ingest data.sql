COPY INTO
[customer]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/customer/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[lineitem]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/lineitem/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[nation]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/nation/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[orders]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/orders/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[part]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/part/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[partsupp]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/partsupp/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[region]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/region/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

COPY INTO
[supplier]
FROM 'https://dwsamples.blob.core.windows.net/tpch1tb/supplier/*.snappy.parquet'
WITH (FILE_TYPE = 'Parquet');
GO

