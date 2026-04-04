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


