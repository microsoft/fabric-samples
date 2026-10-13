/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 12   *************************************/

        select
        	l_shipmode,
        	sum(case
        		when o_orderpriority = '1-URGENT'
        			or o_orderpriority = '2-HIGH'
        			then 1
        		else 0
        	end) as high_line_count,
        	sum(case
        		when o_orderpriority <> '1-URGENT'
        			and o_orderpriority <> '2-HIGH'
        			then 1
        		else 0
        	end) as low_line_count
        from
        	orders,
        	lineitem
        where
        	o_orderkey = l_orderkey
        	and l_shipmode in ('FOB', 'REG AIR')
        	and l_commitdate < l_receiptdate
        	and l_shipdate < l_commitdate
        	and l_receiptdate >=  '1993-01-01' /*  and l_receiptdate >= date '1993-01-01'  */
        	and l_receiptdate <  dateadd(year, +1, '1993-01-01') /*  and l_receiptdate < date '1993-01-01' + interval '1' year  */
        group by
        	l_shipmode
        order by
        	l_shipmode
        option (label = 'TPC-H Query 12');


