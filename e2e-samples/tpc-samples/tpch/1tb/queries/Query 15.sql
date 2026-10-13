/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 15   *************************************/

        drop view if exists revenue0 /*  New line added for error handling.  */
        EXEC ('create view revenue0 (supplier_no, total_revenue) as
        	select
        		l_suppkey,
        		sum(l_extendedprice * (1 - l_discount))
        	from
        		lineitem
        	where
        		l_shipdate >=  ''1995-03-01'' /*  l_shipdate >= date ''1995-03-01''  */
        		and l_shipdate <  dateadd(month, +3, ''1995-03-01'') /*  and l_shipdate < date ''1995-03-01'' + interval ''3'' month  */
        	group by
            l_suppkey')


        select
        	s_suppkey,
        	s_name,
        	s_address,
        	s_phone,
        	total_revenue
        from
        	supplier,
        	revenue0
        where
        	s_suppkey = supplier_no
        	and total_revenue = (
        		select
        			max(total_revenue)
        		from
        			revenue0
        	)
        order by
        	s_suppkey
        option (label = 'TPC-H Query 15');


        drop view if exists revenue0 /*  drop view revenue0  */


