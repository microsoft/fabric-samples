# Data types

This document contains list of all tables, columns and data types used in benchmarks:

- [TPC-H data types](#tpc-h-data-types)

## TPC-H data types

| Table    | Column          | TPCH 1TB                 |
| -------- | --------------- | ------------------------ |
| customer | c_custkey       | bigint NOT  NULL         |
| customer | c_name          | varchar(25)  NOT NULL    |
| customer | c_address       | varchar(40)  NOT NULL    |
| customer | c_nationkey     | int NOT NULL             |
| customer | c_phone         | varchar(15)  NOT NULL    |
| customer | c_acctbal       | decimal(12,  2) NOT NULL |
| customer | c_mktsegment    | varchar(10)  NOT NULL    |
| customer | c_comment       | varchar(117)  NOT NULL   |
| lineitem | l_orderkey      | bigint NOT  NULL         |
| lineitem | l_partkey       | bigint NOT  NULL         |
| lineitem | l_suppkey       | bigint NOT  NULL         |
| lineitem | l_linenumber    | int NOT NULL             |
| lineitem | l_quantity      | decimal(12,  2) NOT NULL |
| lineitem | l_extendedprice | decimal(12,  2) NOT NULL |
| lineitem | l_discount      | decimal(12,  2) NOT NULL |
| lineitem | l_tax           | decimal(12,  2) NOT NULL |
| lineitem | l_returnflag    | varchar(1)  NOT NULL     |
| lineitem | l_linestatus    | varchar(1)  NOT NULL     |
| lineitem | l_shipdate      | date NOT NULL            |
| lineitem | l_commitdate    | date NOT NULL            |
| lineitem | l_receiptdate   | date NOT NULL            |
| lineitem | l_shipinstruct  | varchar(25)  NOT NULL    |
| lineitem | l_shipmode      | varchar(10)  NOT NULL    |
| lineitem | l_comment       | varchar(44)  NOT NULL    |
| nation   | n_nationkey     | int NOT NULL             |
| nation   | n_name          | varchar(25)  NOT NULL    |
| nation   | n_regionkey     | int NOT NULL             |
| nation   | n_comment       | varchar(152)  NOT NULL   |
| orders   | o_orderkey      | bigint NOT  NULL         |
| orders   | o_custkey       | bigint NOT  NULL         |
| orders   | o_orderstatus   | varchar(1)  NOT NULL     |
| orders   | o_totalprice    | decimal(12,  2) NOT NULL |
| orders   | o_orderdate     | date NOT NULL            |
| orders   | o_orderpriority | varchar(15)  NOT NULL    |
| orders   | o_clerk         | varchar(15)  NOT NULL    |
| orders   | o_shippriority  | int NOT NULL             |
| orders   | o_comment       | varchar(79)  NOT NULL    |
| part     | p_partkey       | bigint NOT  NULL         |
| part     | p_name          | varchar(55)  NOT NULL    |
| part     | p_mfgr          | varchar(25)  NOT NULL    |
| part     | p_brand         | varchar(10)  NOT NULL    |
| part     | p_type          | varchar(25)  NOT NULL    |
| part     | p_size          | int NOT NULL             |
| part     | p_container     | varchar(10)  NOT NULL    |
| part     | p_retailprice   | decimal(12,  2) NOT NULL |
| part     | p_comment       | varchar(23)  NOT NULL    |
| partsupp | ps_partkey      | bigint NOT  NULL         |
| partsupp | ps_suppkey      | bigint NOT  NULL         |
| partsupp | ps_availqty     | int NOT NULL             |
| partsupp | ps_supplycost   | decimal(12,  2) NOT NULL |
| partsupp | ps_comment      | varchar(199)  NOT NULL   |
| region   | r_regionkey     | int NOT NULL             |
| region   | r_name          | varchar(25)  NOT NULL    |
| region   | r_comment       | varchar(152)  NOT NULL   |
| supplier | s_suppkey       | bigint NOT  NULL         |
| supplier | s_name          | varchar(25)  NOT NULL    |
| supplier | s_address       | varchar(40)  NOT NULL    |
| supplier | s_nationkey     | int NOT NULL             |
| supplier | s_phone         | varchar(15)  NOT NULL    |
| supplier | s_acctbal       | decimal(12,  2) NOT NULL |
| supplier | s_comment       | varchar(101)  NOT NULL   |
