/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



    /*************************************   TPC-H Query 01   *************************************/

        select
        	l_returnflag,
        	l_linestatus,
        	sum(l_quantity) as sum_qty,
        	sum(l_extendedprice) as sum_base_price,
        	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        	avg(l_quantity) as avg_qty,
        	avg(l_extendedprice) as avg_price,
        	avg(l_discount) as avg_disc,
        count_big(*) as count_order /* count(*) as count_order */
        from
        	lineitem
        where
        	l_shipdate <=  dateadd(day, -88, '1998-12-01') /*  l_shipdate <= date '1998-12-01' - interval '88' day (3)  */
        group by
        	l_returnflag,
        	l_linestatus
        order by
        	l_returnflag,
        	l_linestatus
        option (label = 'TPC-H Query 01');


    /*************************************   TPC-H Query 02   *************************************/

        select top 100
        	s_acctbal,
        	s_name,
        	n_name,
        	p_partkey,
        	p_mfgr,
        	s_address,
        	s_phone,
        	s_comment
        from
        	part,
        	supplier,
        	partsupp,
        	nation,
        	region
        where
        	p_partkey = ps_partkey
        	and s_suppkey = ps_suppkey
        	and p_size = 38
        	and p_type like '%TIN'
        	and s_nationkey = n_nationkey
        	and n_regionkey = r_regionkey
        	and r_name = 'EUROPE'
        	and ps_supplycost = (
        		select
        			min(ps_supplycost)
        		from
        			partsupp,
        			supplier,
        			nation,
        			region
        		where
        			p_partkey = ps_partkey
        			and s_suppkey = ps_suppkey
        			and s_nationkey = n_nationkey
        			and n_regionkey = r_regionkey
        			and r_name = 'EUROPE'
        	)
        order by
        	s_acctbal desc,
        	n_name,
        	s_name,
        	p_partkey
        option (label = 'TPC-H Query 02');


    /*************************************   TPC-H Query 03   *************************************/

        select top 10
        	l_orderkey,
        	sum(l_extendedprice * (1 - l_discount)) as revenue,
        	o_orderdate,
        	o_shippriority
        from
        	customer,
        	orders,
        	lineitem
        where
        	c_mktsegment = 'MACHINERY'
        	and c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and o_orderdate <  '1995-03-24' /*  and o_orderdate < date '1995-03-24'  */
        	and l_shipdate >  '1995-03-24' /*  and l_shipdate > date '1995-03-24'  */
        group by
        	l_orderkey,
        	o_orderdate,
        	o_shippriority
        order by
        	revenue desc,
        	o_orderdate
        option (label = 'TPC-H Query 03');


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


    /*************************************   TPC-H Query 05   *************************************/

        select
        	n_name,
        	sum(l_extendedprice * (1 - l_discount)) as revenue
        from
        	customer,
        	orders,
        	lineitem,
        	supplier,
        	nation,
        	region
        where
        	c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and l_suppkey = s_suppkey
        	and c_nationkey = s_nationkey
        	and s_nationkey = n_nationkey
        	and n_regionkey = r_regionkey
        	and r_name = 'ASIA'
        	and o_orderdate >=  '1994-01-01' /*  and o_orderdate >= date '1994-01-01'  */
        	and o_orderdate <  dateadd(year, +1, '1994-01-01') /*  and o_orderdate < date '1994-01-01' + interval '1' year  */
        group by
        	n_name
        order by
        	revenue desc
        option (label = 'TPC-H Query 05');


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


    /*************************************   TPC-H Query 09   *************************************/

        select
        	nation,
        	o_year,
        	sum(amount) as sum_profit
        from
        	(
        		select
        			n_name as nation,
        datepart(year, o_orderdate) as o_year, /* extract(year from o_orderdate) as o_year, */
        			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        		from
        			part,
        			supplier,
        			lineitem,
        			partsupp,
        			orders,
        			nation
        		where
        			s_suppkey = l_suppkey
        			and ps_suppkey = l_suppkey
        			and ps_partkey = l_partkey
        			and p_partkey = l_partkey
        			and o_orderkey = l_orderkey
        			and s_nationkey = n_nationkey
        			and p_name like '%bisque%'
        	) as profit
        group by
        	nation,
        	o_year
        order by
        	nation,
        	o_year desc
        option (label = 'TPC-H Query 09');


    /*************************************   TPC-H Query 10   *************************************/

        select top 20
        	c_custkey,
        	c_name,
        	sum(l_extendedprice * (1 - l_discount)) as revenue,
        	c_acctbal,
        	n_name,
        	c_address,
        	c_phone,
        	c_comment
        from
        	customer,
        	orders,
        	lineitem,
        	nation
        where
        	c_custkey = o_custkey
        	and l_orderkey = o_orderkey
        	and o_orderdate >=  '1993-06-01' /*  and o_orderdate >= date '1993-06-01'  */
        	and o_orderdate <  dateadd(month, +3, '1993-06-01') /*  and o_orderdate < date '1993-06-01' + interval '3' month  */
        	and l_returnflag = 'R'
        	and c_nationkey = n_nationkey
        group by
        	c_custkey,
        	c_name,
        	c_acctbal,
        	c_phone,
        	n_name,
        	c_address,
        	c_comment
        order by
        	revenue desc
        option (label = 'TPC-H Query 10');


    /*************************************   TPC-H Query 11   *************************************/

        select
        	ps_partkey,
        	sum(ps_supplycost * ps_availqty) as value
        from
        	partsupp,
        	supplier,
        	nation
        where
        	ps_suppkey = s_suppkey
        	and s_nationkey = n_nationkey
        	and n_name = 'CHINA'
        group by
        	ps_partkey having
        		sum(ps_supplycost * ps_availqty) > (
        			select
        				sum(ps_supplycost * ps_availqty) * 0.0000001000
        			from
        				partsupp,
        				supplier,
        				nation
        			where
        				ps_suppkey = s_suppkey
        				and s_nationkey = n_nationkey
        				and n_name = 'CHINA'
        		)
        order by
        	value desc
        option (label = 'TPC-H Query 11');


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


    /*************************************   TPC-H Query 13   *************************************/

        select
        	c_count,
        	count(*) as custdist
        from
        	(
        		select
        			c_custkey,
        			count(o_orderkey)
        		from
        			customer left outer join orders on
        				c_custkey = o_custkey
        				and o_comment not like '%special%packages%'
        		group by
        			c_custkey
        	) as c_orders (c_custkey, c_count)
        group by
        	c_count
        order by
        	custdist desc,
        	c_count desc
        option (label = 'TPC-H Query 13');


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


    /*************************************   TPC-H Query 17   *************************************/

        select
        	sum(l_extendedprice) / 7.0 as avg_yearly
        from
        	lineitem,
        	part
        where
        	p_partkey = l_partkey
        	and p_brand = 'Brand#55'
        	and p_container = 'SM PACK'
        	and l_quantity < (
        		select
        			0.2 * avg(l_quantity)
        		from
        			lineitem
        		where
        			l_partkey = p_partkey
        	)
        option (label = 'TPC-H Query 17');


    /*************************************   TPC-H Query 18   *************************************/

        select top 100
        	c_name,
        	c_custkey,
        	o_orderkey,
        	o_orderdate,
        	o_totalprice,
        	sum(l_quantity)
        from
        	customer,
        	orders,
        	lineitem
        where
        	o_orderkey in (
        		select
        			l_orderkey
        		from
        			lineitem
        		group by
        			l_orderkey having
        				sum(l_quantity) > 315
        	)
        	and c_custkey = o_custkey
        	and o_orderkey = l_orderkey
        group by
        	c_name,
        	c_custkey,
        	o_orderkey,
        	o_orderdate,
        	o_totalprice
        order by
        	o_totalprice desc,
        	o_orderdate
        option (label = 'TPC-H Query 18');


    /*************************************   TPC-H Query 19   *************************************/

        select
        	sum(l_extendedprice* (1 - l_discount)) as revenue
        from
        	lineitem,
        	part
        where
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#13'
        		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        		and l_quantity >= 8 and l_quantity <= 8 + 10
        		and p_size between 1 and 5
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        	or
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#51'
        		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        		and l_quantity >= 19 and l_quantity <= 19 + 10
        		and p_size between 1 and 10
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        	or
        	(
        		p_partkey = l_partkey
        		and p_brand = 'Brand#41'
        		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        		and l_quantity >= 22 and l_quantity <= 22 + 10
        		and p_size between 1 and 15
        		and l_shipmode in ('AIR', 'AIR REG')
        		and l_shipinstruct = 'DELIVER IN PERSON'
        	)
        option (label = 'TPC-H Query 19');


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


    /*************************************   TPC-H Query 22   *************************************/

        select
        	cntrycode,
        	count(*) as numcust,
        	sum(c_acctbal) as totacctbal
        from
        	(
        		select
        substring(c_phone, 1, 2) as cntrycode, /* substring(c_phone from 1 for 2) as cntrycode, */
        			c_acctbal
        		from
        			customer
        		where
        substring(c_phone, 1, 2) in /* substring(c_phone from 1 for 2) in */
        				('10', '29', '12', '30', '32', '18', '17')
        			and c_acctbal > (
        				select
        					avg(c_acctbal)
        				from
        					customer
        				where
        					c_acctbal > 0.00
        and substring(c_phone, 1, 2) in /* and substring(c_phone from 1 for 2) in */
        						('10', '29', '12', '30', '32', '18', '17')
        			)
        			and not exists (
        				select
        					*
        				from
        					orders
        				where
        					o_custkey = c_custkey
        			)
        	) as custsale
        group by
        	cntrycode
        order by
        	cntrycode
        option (label = 'TPC-H Query 22');


