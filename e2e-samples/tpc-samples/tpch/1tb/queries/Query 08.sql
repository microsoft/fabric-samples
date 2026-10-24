/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 08   *************************************/

        select
        	o_year,
        	sum(case
        		when nation = 'ALGERIA' then volume
        		else 0
        	end) / sum(volume) as mkt_share
        from
        	(
        		select
        datepart(year, o_orderdate) as o_year, /* extract(year from o_orderdate) as o_year, */
        			l_extendedprice * (1 - l_discount) as volume,
        			n2.n_name as nation
        		from
        			part,
        			supplier,
        			lineitem,
        			orders,
        			customer,
        			nation n1,
        			nation n2,
        			region
        		where
        			p_partkey = l_partkey
        			and s_suppkey = l_suppkey
        			and l_orderkey = o_orderkey
        			and o_custkey = c_custkey
        			and c_nationkey = n1.n_nationkey
        			and n1.n_regionkey = r_regionkey
        			and r_name = 'AFRICA'
        			and s_nationkey = n2.n_nationkey
        			and o_orderdate between  '1995-01-01' and '1996-12-31' /*  and o_orderdate between date '1995-01-01' and date '1996-12-31'  */
        			and p_type = 'STANDARD BURNISHED STEEL'
        	) as all_nations
        group by
        	o_year
        order by
        	o_year
        option (label = 'TPC-H Query 08');


