/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 21   *************************************/

        select top 100
        	s_name,
        	count(*) as numwait
        from
        	supplier,
        	lineitem l1,
        	orders,
        	nation
        where
        	s_suppkey = l1.l_suppkey
        	and o_orderkey = l1.l_orderkey
        	and o_orderstatus = 'F'
        	and l1.l_receiptdate > l1.l_commitdate
        	and exists (
        		select
        			*
        		from
        			lineitem l2
        		where
        			l2.l_orderkey = l1.l_orderkey
        			and l2.l_suppkey <> l1.l_suppkey
        	)
        	and not exists (
        		select
        			*
        		from
        			lineitem l3
        		where
        			l3.l_orderkey = l1.l_orderkey
        			and l3.l_suppkey <> l1.l_suppkey
        			and l3.l_receiptdate > l3.l_commitdate
        	)
        	and s_nationkey = n_nationkey
        	and n_name = 'SAUDI ARABIA'
        group by
        	s_name
        order by
        	numwait desc,
        	s_name
        option (label = 'TPC-H Query 21');


