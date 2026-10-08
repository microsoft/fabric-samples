/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 13   *************************************/

        select
        	c_count,
        	count(*) as custdist
        from
        	(
        		select
        			c_custkey,
        			count(o_orderkey)
        		from
        			customer left outer join orders on
        				c_custkey = o_custkey
        				and o_comment not like '%special%packages%'
        		group by
        			c_custkey
        	) as c_orders (c_custkey, c_count)
        group by
        	c_count
        order by
        	custdist desc,
        	c_count desc
        option (label = 'TPC-H Query 13');


