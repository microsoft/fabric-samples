/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 04   *************************************/

        select
        	o_orderpriority,
        count_big(*) as order_count /* count(*) as order_count */
        from
        	orders
        where
        	o_orderdate >=  '1996-09-01' /*  o_orderdate >= date '1996-09-01'  */
        	and o_orderdate <  dateadd(month, +3, '1996-09-01') /*  and o_orderdate < date '1996-09-01' + interval '3' month  */
        	and exists (
        		select
        			*
        		from
        			lineitem
        		where
        			l_orderkey = o_orderkey
        			and l_commitdate < l_receiptdate
        	)
        group by
        	o_orderpriority
        order by
        	o_orderpriority
        option (label = 'TPC-H Query 04');


