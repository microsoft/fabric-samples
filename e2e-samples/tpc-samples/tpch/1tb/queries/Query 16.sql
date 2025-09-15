/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 16   *************************************/

        select
        	p_brand,
        	p_type,
        	p_size,
        	count(distinct ps_suppkey) as supplier_cnt
        from
        	partsupp,
        	part
        where
        	p_partkey = ps_partkey
        	and p_brand <> 'Brand#15'
        	and p_type not like 'ECONOMY BRUSHED%'
        	and p_size in (7, 22, 42, 4, 39, 1, 41, 45)
        	and ps_suppkey not in (
        		select
        			s_suppkey
        		from
        			supplier
        		where
        			s_comment like '%Customer%Complaints%'
        	)
        group by
        	p_brand,
        	p_type,
        	p_size
        order by
        	supplier_cnt desc,
        	p_brand,
        	p_type,
        	p_size
        option (label = 'TPC-H Query 16');


