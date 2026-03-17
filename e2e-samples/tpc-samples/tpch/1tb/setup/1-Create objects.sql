DROP TABLE IF EXISTS dbo.customer;
DROP TABLE IF EXISTS dbo.lineitem;
DROP TABLE IF EXISTS dbo.nation;
DROP TABLE IF EXISTS dbo.orders;
DROP TABLE IF EXISTS dbo.part;
DROP TABLE IF EXISTS dbo.partsupp;
DROP TABLE IF EXISTS dbo.region;
DROP TABLE IF EXISTS dbo.supplier;


CREATE TABLE dbo.customer
	(
		c_custkey 			BIGINT 			NOT NULL,
		c_name 				VARCHAR(25) 	NOT NULL,
		c_address 			VARCHAR(40) 	NOT NULL,
		c_nationkey 		INT 			NOT NULL,
		c_phone 			VARCHAR(15) 	NOT NULL,
		c_acctbal 			DECIMAL(12, 2) 	NOT NULL,
		c_mktsegment 		VARCHAR(10)		NOT NULL,
		c_comment 			VARCHAR(117) 	NOT NULL		
	);


CREATE TABLE dbo.lineitem
	(
		l_orderkey 			BIGINT 			NOT NULL,
		l_partkey 			BIGINT 			NOT NULL,
		l_suppkey 			BIGINT 			NOT NULL,
		l_linenumber 		INT				NOT NULL,
		l_quantity 			DECIMAL(12, 2) 	NOT NULL,
		l_extendedprice 	DECIMAL(12, 2) 	NOT NULL,
		l_discount 			DECIMAL(12, 2) 	NOT NULL,
		l_tax 				DECIMAL(12, 2) 	NOT NULL,
		l_returnflag 		VARCHAR(1) 		NOT NULL,
		l_linestatus 		VARCHAR(1) 		NOT NULL,
		l_shipdate 			DATE 			NOT NULL,
		l_commitdate 		DATE 			NOT NULL,
		l_receiptdate 		DATE 			NOT NULL,
		l_shipinstruct 		VARCHAR(25) 	NOT NULL,
		l_shipmode 			VARCHAR(10) 	NOT NULL,
		l_comment 			VARCHAR(44) 	NOT NULL	
	);


CREATE TABLE dbo.nation
	(
		n_nationkey 		INT 			NOT NULL,
		n_name 				VARCHAR(25) 	NOT NULL,
		n_regionkey 		INT 			NOT NULL,
		n_comment 			VARCHAR(152) 	NOT NULL		
	);


CREATE TABLE dbo.orders
	(
		o_orderkey 			BIGINT			NOT NULL,
		o_custkey 			BIGINT 			NOT NULL,
		o_orderstatus 		VARCHAR(1) 		NOT NULL,
		o_totalprice 		DECIMAL(12, 2) 	NOT NULL,
		o_orderdate 		DATE 			NOT NULL,
		o_orderpriority 	VARCHAR(15) 	NOT NULL,
		o_clerk 			VARCHAR(15) 	NOT NULL,
		o_shippriority 		INT 			NOT NULL,
		o_comment 			VARCHAR(79) 	NOT NULL	
	);


CREATE TABLE dbo.part
	(
		p_partkey 			BIGINT 			NOT NULL,
		p_name 				VARCHAR(55) 	NOT NULL,
		p_mfgr 				VARCHAR(25) 	NOT NULL,
		p_brand 			VARCHAR(10) 	NOT NULL,
		p_type 				VARCHAR(25) 	NOT NULL,
		p_size 				INT 			NOT NULL,
		p_container 		VARCHAR(10) 	NOT NULL,
		p_retailprice 		DECIMAL(12, 2) 	NOT NULL,
		p_comment 			VARCHAR(23) 	NOT NULL	
	);


CREATE TABLE dbo.partsupp
	(
		ps_partkey 			BIGINT 			NOT NULL,
		ps_suppkey 			BIGINT 			NOT NULL,
		ps_availqty 		INT 			NOT NULL,
		ps_supplycost 		DECIMAL(12, 2) 	NOT NULL,
		ps_comment 			VARCHAR(199) 	NOT NULL	
	);


CREATE TABLE dbo.region
	(
		r_regionkey 		INT 			NOT NULL,
		r_name 				VARCHAR(25) 	NOT NULL,
		r_comment 			VARCHAR(152) 	NOT NULL		
	);


CREATE TABLE dbo.supplier
	(
		s_suppkey 			BIGINT 			NOT NULL,
		s_name 				VARCHAR(25)		NOT NULL,
		s_address 			VARCHAR(40) 	NOT NULL,
		s_nationkey 		INT 			NOT NULL,
		s_phone 			VARCHAR(15) 	NOT NULL,
		s_acctbal 			DECIMAL(12, 2)	NOT NULL,
		s_comment 			VARCHAR(101) 	NOT NULL		
	);