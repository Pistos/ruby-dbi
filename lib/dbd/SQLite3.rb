#--
# DBD::SQLite3
# 
# copyright (c) 2005 Jun Mukai <mukai@jmuk.org>
# Compatibility patches by Erik Hollensbe <erik@hollensbe.org>
# 
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions 
# are met:
# 1. Redistributions of source code must retain the above copyright 
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in the 
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
# THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#++

begin
  require 'rubygems'
  gem 'sqlite3-ruby'
  gem 'dbi'
rescue LoadError
end

require 'dbi'
require 'sqlite3'
require 'sqlite3/version'

module DBI
    module DBD
        #
        # DBD::SQLite3 - Database Driver for SQLite versions 3.x
        #
        # Requires DBI and the 'sqlite3-ruby' gem to work.
        #
        # Only things that extend DBI's results are documented.
        #
        module SQLite3
            VERSION = "1.2.3"
            DESCRIPTION = "SQLite 3.x DBD for DBI"

            #
            # returns 'SQLite3'
            #
            # See DBI::TypeUtil#convert for more information.
            #
            def self.driver_name
                "SQLite3"
            end

            #
            # Validates that the SQL has no literal NUL characters. (ASCII 0)
            #
            # SQLite apparently really hates it when you do that.
            #
            # It will raise DBI::DatabaseError should it find any.
            #
            def self.parse_type(type_name)
                # FIXME plucked from SQLite driver, this needs to be in DBI proper 
                return ['varchar'] unless type_name
                type_name.match(/^([^\(\s]+)(\s*\(\s*(\d+)(,(\d+))?\s*\)\s*)?$/)
            end

            #
            # See DBI::BaseDriver.
            #
            class Driver < DBI::BaseDriver
                def initialize
                    @dbs = []
                    super "0.4.0"
                end

                def connect(dbname, user, auth, attr)
                    raise DBI::InterfaceError, "dbname must be a string" unless dbname.kind_of? String
                    raise DBI::InterfaceError, "dbname must have some length" unless dbname.length > 0
                    raise DBI::InterfaceError, "attrs must be a hash" unless attr.kind_of? Hash
                    db = DBI::DBD::SQLite3::Database.new(dbname, attr)
                    @dbs.push(db)
                    db
                end

                def disconnect_all()
                    @dbs.each{|db| db.disconnect()}
                end
            end
        end
    end
end

require 'dbd/sqlite3/database'
require 'dbd/sqlite3/statement'

DBI::TypeUtil.register_conversion(DBI::DBD::SQLite3.driver_name) do |obj|
    newobj = case obj
             when ::TrueClass
                '1'
             when ::FalseClass
                '0'
             else
                 # SQLite3 is managing its own conversion right now, until I'm happy let's keep it that way
                 obj.dup rescue obj
             end
    if newobj.object_id == obj.object_id
        [newobj, true]
    else
        [newobj, false]
    end
end
