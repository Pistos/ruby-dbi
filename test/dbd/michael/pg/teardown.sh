#! /bin/bash

CONFIG_PATH=`dirname $0`
. $CONFIG_PATH/config.sh

psql $DB $USER <<EOF
DROP TABLE $TABLE1;
DROP TABLE $TABLE2;
EOF
