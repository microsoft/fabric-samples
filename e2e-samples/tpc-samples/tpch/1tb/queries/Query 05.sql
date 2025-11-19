/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 05   *************************************/

        select
        	n_name,
        	sum(l_extendedprice * (1 - l_discount)) as revenue
        from
        	customer,
        	orders,
        	lineitem,
        	supplier,
        	nation,
        	region
        where
        	c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and l_suppkey = s_suppkey
        	and c_nationkey = s_nationkey
        	and s_nationkey = n_nationkey
        	and n_regionkey = r_regionkey
        	and r_name = 'ASIA'
        	and o_orderdate >=  '1994-01-01' /*  and o_orderdate >= date '1994-01-01'  */
        	and o_orderdate <  dateadd(year, +1, '1994-01-01') /*  and o_orderdate < date '1994-01-01' + interval '1' year  */
        group by
        	n_name
        order by
        	revenue desc
        option (label = 'TPC-H Query 05');


