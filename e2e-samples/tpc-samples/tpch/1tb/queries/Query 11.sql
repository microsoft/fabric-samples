/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 11   *************************************/

        select
        	ps_partkey,
        	sum(ps_supplycost * ps_availqty) as value
        from
        	partsupp,
        	supplier,
        	nation
        where
        	ps_suppkey = s_suppkey
        	and s_nationkey = n_nationkey
        	and n_name = 'CHINA'
        group by
        	ps_partkey having
        		sum(ps_supplycost * ps_availqty) > (
        			select
        				sum(ps_supplycost * ps_availqty) * 0.0000001000
        			from
        				partsupp,
        				supplier,
        				nation
        			where
        				ps_suppkey = s_suppkey
        				and s_nationkey = n_nationkey
        				and n_name = 'CHINA'
        		)
        order by
        	value desc
        option (label = 'TPC-H Query 11');


