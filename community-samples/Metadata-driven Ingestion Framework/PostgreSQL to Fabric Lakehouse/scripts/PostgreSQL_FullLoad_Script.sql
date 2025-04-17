--Drop table customer_test

create table customer_test(
srno int, cusname varchar(10),created_at timestamp
);

---Full Load
insert into customer_test values (1,'c1',(NOW() - interval '6 day'));
insert into customer_test values (2,'c2',(NOW() - interval '5 day'));
insert into customer_test values (3,'c3',(NOW() - interval '4 day'));
insert into customer_test values (4,'c4',(NOW() - interval '3 day'));
insert into customer_test values (5,'c5',(NOW() - interval '2 day'));
insert into customer_test values (6,'c6',(NOW() - interval '1 day'));
insert into customer_test values (7,'c7',NOW());
 
select * from customer_test;

