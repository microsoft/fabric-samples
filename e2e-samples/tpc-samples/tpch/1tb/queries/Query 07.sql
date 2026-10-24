/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 07   *************************************/

        select
        	supp_nation,
        	cust_nation,
        	l_year,
        	sum(volume) as revenue
        from
        	(
        		select
        			n1.n_name as supp_nation,
        			n2.n_name as cust_nation,
        datepart(year, l_shipdate) as l_year, /* extract(year from l_shipdate) as l_year, */
        			l_extendedprice * (1 - l_discount) as volume
        		from
        			supplier,
        			lineitem,
        			orders,
        			customer,
        			nation n1,
        			nation n2
        		where
        			s_suppkey = l_suppkey
        			and o_orderkey = l_orderkey
        			and c_custkey = o_custkey
        			and s_nationkey = n1.n_nationkey
        			and c_nationkey = n2.n_nationkey
        			and (
        				(n1.n_name = 'FRANCE' and n2.n_name = 'ALGERIA')
        				or (n1.n_name = 'ALGERIA' and n2.n_name = 'FRANCE')
        			)
        			and l_shipdate between  '1995-01-01' and '1996-12-31' /*  and l_shipdate between date '1995-01-01' and date '1996-12-31'  */
        	) as shipping
        group by
        	supp_nation,
        	cust_nation,
        	l_year
        order by
        	supp_nation,
        	cust_nation,
        	l_year
        option (label = 'TPC-H Query 07');


