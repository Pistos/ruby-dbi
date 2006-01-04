require "mkmf"

dir_config "SQLite", "/usr/local"

if find_library("sqlite", "sqlite_open", "/usr/local/lib", "/usr/pkg/lib") and have_header("sqlite.h")
  create_makefile "SQLite"
end
