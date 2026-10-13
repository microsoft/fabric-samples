/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 18   *************************************/

        select top 100
        	c_name,
        	c_custkey,
        	o_orderkey,
        	o_orderdate,
        	o_totalprice,
        	sum(l_quantity)
        from
        	customer,
        	orders,
        	lineitem
        where
        	o_orderkey in (
        		select
        			l_orderkey
        		from
        			lineitem
        		group by
        			l_orderkey having
        				sum(l_quantity) > 315
        	)
        	and c_custkey = o_custkey
        	and o_orderkey = l_orderkey
        group by
        	c_name,
        	c_custkey,
        	o_orderkey,
        	o_orderdate,
        	o_totalprice
        order by
        	o_totalprice desc,
        	o_orderdate
        option (label = 'TPC-H Query 18');


