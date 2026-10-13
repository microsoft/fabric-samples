/*************************************   Notes   *************************************/
/*
    Generated on 2024-10-03
    This is the TPC-H 1000 GB (TB_001) scale factor queries modified for Fabric DW T-SQL syntax.

    TPC-H Parameter Substitution (Version 3.0.0 build 0)
    Using 81310311 as a seed to the RNG
*/



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


