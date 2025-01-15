/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 03   *************************************/

        select top 10
        	l_orderkey,
        	sum(l_extendedprice * (1 - l_discount)) as revenue,
        	o_orderdate,
        	o_shippriority
        from
        	customer,
        	orders,
        	lineitem
        where
        	c_mktsegment = 'MACHINERY'
        	and c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and o_orderdate <  '1995-03-24' /*  and o_orderdate < date '1995-03-24'  */
        	and l_shipdate >  '1995-03-24' /*  and l_shipdate > date '1995-03-24'  */
        group by
        	l_orderkey,
        	o_orderdate,
        	o_shippriority
        order by
        	revenue desc,
        	o_orderdate
        option (label = 'TPC-H Query 03');


