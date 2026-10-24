/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 17   *************************************/

        select
        	sum(l_extendedprice) / 7.0 as avg_yearly
        from
        	lineitem,
        	part
        where
        	p_partkey = l_partkey
        	and p_brand = 'Brand#55'
        	and p_container = 'SM PACK'
        	and l_quantity < (
        		select
        			0.2 * avg(l_quantity)
        		from
        			lineitem
        		where
        			l_partkey = p_partkey
        	)
        option (label = 'TPC-H Query 17');


