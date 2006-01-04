require 'mkmf'

dir_config("freetds", "/usr/local/freetds")
$libs = "-ltds"

create_makefile("dbd_sybase")