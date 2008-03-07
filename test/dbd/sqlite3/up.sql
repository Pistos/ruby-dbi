create table names (name varchar(255), age integer);

insert into names (name, age) values ("Bob", 21);
insert into names (name, age) values ("Joe", 19);
insert into names (name, age) values ("Jim", 30);

create view view_names as select * from names;

create table blob_test (name varchar(255));

create table boolean_test (num integer, mybool boolean);
