/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 14   *************************************/

        select
        	100.00 * sum(case
        		when p_type like 'PROMO%'
        			then l_extendedprice * (1 - l_discount)
        		else 0
        	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from
        	lineitem,
        	part
        where
        	l_partkey = p_partkey
        	and l_shipdate >=  '1993-11-01' /*  and l_shipdate >= date '1993-11-01'  */
        	and l_shipdate <  dateadd(month, +1, '1993-11-01') /*  and l_shipdate < date '1993-11-01' + interval '1' month  */
        option (label = 'TPC-H Query 14');


