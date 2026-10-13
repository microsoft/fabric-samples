/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 20   *************************************/

        select
        	s_name,
        	s_address
        from
        	supplier,
        	nation
        where
        	s_suppkey in (
        		select
        			ps_suppkey
        		from
        			partsupp
        		where
        			ps_partkey in (
        				select
        					p_partkey
        				from
        					part
        				where
        					p_name like 'ivory%'
        			)
        			and ps_availqty > (
        				select
        					0.5 * sum(l_quantity)
        				from
        					lineitem
        				where
        					l_partkey = ps_partkey
        					and l_suppkey = ps_suppkey
        					and l_shipdate >=  '1997-01-01' /*  and l_shipdate >= date '1997-01-01'  */
        					and l_shipdate <  dateadd(year, +1, '1997-01-01') /*  and l_shipdate < date '1997-01-01' + interval '1' year  */
        			)
        	)
        	and s_nationkey = n_nationkey
        	and n_name = 'ALGERIA'
        order by
        	s_name
        option (label = 'TPC-H Query 20');


