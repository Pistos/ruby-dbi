create table names (name varchar(255), age integer);
---
insert into names (name, age) values ("Bob", 21);
---
insert into names (name, age) values ("Joe", 19);
---
insert into names (name, age) values ("Jim", 30);
---
create table precision_test (text_field varchar(20) primary key not null, integer_field integer, decimal_field decimal(2,1), numeric_field numeric(30,6));
---
create view view_names as select * from names;
---
create table blob_test (name varchar(255));
---
create table boolean_test (num integer, mybool boolean);
---
create table time_test (mytime time);
---
create table timestamp_test (mytimestamp timestamp);
---
create table bit_test (mybit bit);
---
create table field_types_test (foo integer not null primary key default 1);
---
create table db_specific_types_test (dbl double);
