create table names (
    name varchar(255) not null,
    age integer not null
);

insert into names (name, age) values ('Joe', 19);
insert into names (name, age) values ('Jim', 30);
insert into names (name, age) values ('Bob', 21);

CREATE TABLE blob_test (name VARCHAR(30), data OID);

create view view_names as select * from names;

create or replace function test_insert (varchar(255), integer) 
    returns integer 
    language sql 
    as 'insert into names (name, age) values ($1, $2); select age from names where name = $1';
