---Incremental Load
insert into customer_test values (8,'c8',(NOW()+ interval '1 day'));
insert into customer_test values (9,'c9',(NOW()+ interval '2 day'));

select * from customer_test;