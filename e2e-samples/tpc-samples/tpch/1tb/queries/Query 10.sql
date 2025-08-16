/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 10   *************************************/

        select top 20
        	c_custkey,
        	c_name,
        	sum(l_extendedprice * (1 - l_discount)) as revenue,
        	c_acctbal,
        	n_name,
        	c_address,
        	c_phone,
        	c_comment
        from
        	customer,
        	orders,
        	lineitem,
        	nation
        where
        	c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and o_orderdate >=  '1993-06-01' /*  and o_orderdate >= date '1993-06-01'  */
        	and o_orderdate <  dateadd(month, +3, '1993-06-01') /*  and o_orderdate < date '1993-06-01' + interval '3' month  */
        	and l_returnflag = 'R'
        	and c_nationkey = n_nationkey
        group by
        	c_custkey,
        	c_name,
        	c_acctbal,
        	c_phone,
        	n_name,
        	c_address,
        	c_comment
        order by
        	revenue desc
        option (label = 'TPC-H Query 10');


