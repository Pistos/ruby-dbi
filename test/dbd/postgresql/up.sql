create table names (
    name varchar(255),
    age integer
);
---
insert into names (name, age) values ('Joe', 19);
---
insert into names (name, age) values ('Jim', 30);
---
insert into names (name, age) values ('Bob', 21);
---
CREATE TABLE blob_test (name VARCHAR(30), data OID);
---
create view view_names as select * from names;
---
create or replace function test_insert (varchar(255), integer) 
    returns integer 
    language sql 
    as 'insert into names (name, age) values ($1, $2); select age from names where name = $1';
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
create table array_test (foo integer[], bar integer[3], baz integer[3][3]);
---
create table bytea_test (foo bytea);
---
create schema schema1;
---
create schema schema2;
---
create table schema1.tbl (foo integer);
---
create table schema2.tbl (bar integer);
