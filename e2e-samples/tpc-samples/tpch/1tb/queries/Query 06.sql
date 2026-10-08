/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 06   *************************************/

        select
        	sum(l_extendedprice * l_discount) as revenue
        from
        	lineitem
        where
        	l_shipdate >=  '1994-01-01' /*  l_shipdate >= date '1994-01-01'  */
        	and l_shipdate <  dateadd(year, +1, '1994-01-01') /*  and l_shipdate < date '1994-01-01' + interval '1' year  */
        	and l_discount between 0.04 - 0.01 and 0.04 + 0.01
        	and l_quantity < 24
        option (label = 'TPC-H Query 06');


