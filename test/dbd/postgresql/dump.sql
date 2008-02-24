create table names (
    name varchar(255) not null,
    age integer not null
);

insert into names (name, age) values ('Bob', 21);
insert into names (name, age) values ('Charlie', 22);

CREATE TABLE blob_test (name VARCHAR(30), data OID);
