require 'mkmf'

dir_config 'sqlite'

if find_library('sqlite', 'sqlite_open') and have_header('sqlite.h')
   create_makefile 'SQLite'
end