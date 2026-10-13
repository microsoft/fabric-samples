/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 02   *************************************/

        select top 100
        	s_acctbal,
        	s_name,
        	n_name,
        	p_partkey,
        	p_mfgr,
        	s_address,
        	s_phone,
        	s_comment
        from
        	part,
        	supplier,
        	partsupp,
        	nation,
        	region
        where
        	p_partkey = ps_partkey
        	and s_suppkey = ps_suppkey
        	and p_size = 38
        	and p_type like '%TIN'
        	and s_nationkey = n_nationkey
        	and n_regionkey = r_regionkey
        	and r_name = 'EUROPE'
        	and ps_supplycost = (
        		select
        			min(ps_supplycost)
        		from
        			partsupp,
        			supplier,
        			nation,
        			region
        		where
        			p_partkey = ps_partkey
        			and s_suppkey = ps_suppkey
        			and s_nationkey = n_nationkey
        			and n_regionkey = r_regionkey
        			and r_name = 'EUROPE'
        	)
        order by
        	s_acctbal desc,
        	n_name,
        	s_name,
        	p_partkey
        option (label = 'TPC-H Query 02');


