create table names (
    name varchar(255),
    age integer
) Engine=InnoDB;
---
insert into names (name, age) values ('Joe', 19);
---
insert into names (name, age) values ('Jim', 30);
---
insert into names (name, age) values ('Bob', 21);
---
create table precision_test (text_field varchar(20) primary key not null, integer_field integer, decimal_field decimal(2,1), numeric_field numeric(30,6));
---
CREATE TABLE blob_test (name VARCHAR(30), data BLOB) Engine=InnoDB;
---
create view view_names as select * from names;
---
create table boolean_test (num integer, mybool boolean) Engine=InnoDB;
---
create table time_test (mytime time) Engine=InnoDB;
---
create table timestamp_test (mytimestamp timestamp) Engine=InnoDB;
---
create table bit_test (mybit bit) Engine=InnoDB;
---
create table field_types_test (foo integer not null primary key default 1);
---
create table db_specific_types_test (ts timestamp, dt date);
