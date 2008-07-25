###############################################################################
#
# DBD::SQLite - a DBD for SQLite for versions < 3
#
# Uses Jamis Buck's 'sqlite-ruby' driver to interface with SQLite directly
#
# (c) 2008 Erik Hollensbe & Christopher Maujean.
#
# TODO
#
# fetch_scroll implementation?
# columns and column_info differ too much and have too much copied code, refactor
# there are probably some edge cases with transactions
#
################################################################################

begin
    require 'rubygems'
    gem 'sqlite'
rescue Exception => e
end

require 'sqlite'

module DBI
    module DBD
        class SQLite
            VERSION = "0.1"
            USED_DBD_VERSION = "0.1"
            DESCRIPTION = "SQLite 2.x DBI DBD"

            def self.driver_name
                "SQLite"
            end

            # XXX I'm starting to think this is less of a problem with SQLite
            # and more with the old C DBD
            def self.check_sql(sql)
                raise DBI::DatabaseError, "Bad SQL: SQL cannot contain nulls" if sql =~ /\0/
            end

            def self.parse_type(type_name)
                type_name.match(/^([^\(]+)(\((\d+)(,(\d+))?\))?$/)
            end

            class Driver < DBI::BaseDriver
                def initialize
                    super USED_DBD_VERSION
                end

                def connect(dbname, user, auth, attr_hash)
                    return Database.new(dbname, user, auth, attr_hash)
                end
            end
        end
    end
end

require 'dbd/sqlite/database'
require 'dbd/sqlite/statement'

DBI::TypeUtil.register_conversion(DBI::DBD::SQLite.driver_name) do |obj|
    case obj
    when ::NilClass
        "NULL"
    else 
        obj
    end
end
