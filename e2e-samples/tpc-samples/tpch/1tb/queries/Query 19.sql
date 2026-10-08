/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 19   *************************************/

        select
        	sum(l_extendedprice* (1 - l_discount)) as revenue
        from
        	lineitem,
        	part
        where
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#13'
        		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        		and l_quantity >= 8 and l_quantity <= 8 + 10
        		and p_size between 1 and 5
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        	or
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#51'
        		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        		and l_quantity >= 19 and l_quantity <= 19 + 10
        		and p_size between 1 and 10
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        	or
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#41'
        		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        		and l_quantity >= 22 and l_quantity <= 22 + 10
        		and p_size between 1 and 15
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        option (label = 'TPC-H Query 19');


