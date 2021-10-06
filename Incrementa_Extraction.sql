use ETL;

create table customer(c_id int primary key not null, c_name varchar(20), c_city varchar(20),
					  crt_date datetime, updt_date datetime);
                      
insert into customer(c_id, c_name, c_city, crt_date) values
(4, 'Allwin', 'Banglore',now());

select * from customer;

update customer set c_city = 'Pune', updt_date = now() where c_id = 2;

create table cust_temp(s_c_id int primary key auto_increment, c_id int, c_name varchar(20), 
					   c_city varchar(20), crt_date datetime, updt_date datetime);
                       

insert into cust_temp(c_id, c_name, c_city, crt_date) 
select c_id, c_name, c_city, now() from customer;

select * from cust_temp;

select * from cust_temp where s_c_id in (select max(s_c_id) from cust_temp group by c_id);

select * from cust_temp group by c_id having max(s_c_id);

delete from cust_temp;

alter table cust_temp auto_increment = 1;

alter table cust_temp add column crt_date datetime;

insert into cust_temp(c_id, c_name, c_city) values (5,'Manick','Del');