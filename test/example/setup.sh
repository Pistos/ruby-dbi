#!/bin/sh

CONFIG_PATH=`dirname $0`
. $CONFIG_PATH/config.sh

echo DB $DB
echo USER $USER
echo PW $PW
echo TABLE1 $TABLE1

psql $DB $USER <<EOF
BEGIN WORK;

CREATE TABLE $TABLE1 (
   name VARCHAR(20),
   age  INTEGER);

INSERT INTO $TABLE1 (name, age) VALUES ('Adam',    20);
INSERT INTO $TABLE1 (name, age) VALUES ('Bob',     21);
INSERT INTO $TABLE1 (name, age) VALUES ('Charlie', 22);

CREATE TABLE $TABLE2 (
   fint   INTEGER,
   fvchar VARCHAR(20));

COMMIT WORK;
EOF
