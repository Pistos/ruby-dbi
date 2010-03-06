#--
# Copyright (c) 2001, 2002 Michael Neumann <neumann@s-direktnet.de>
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
#
# $Id$
#++

begin
    require 'rubygems'
    gem 'mysql'
    gem 'dbi'
rescue LoadError => e
end

require 'dbi'
require "mysql"
require "thread"   # for Mutex

module DBI
    module DBD
        #
        # DBD::Mysql - Database Driver for the MySQL database system.
        #
        # Requires DBI and the 'mysql' gem or package to work.
        #
        # Only things that extend DBI's results are documented.
        #
        module Mysql
            VERSION          = "0.4.4"
            DESCRIPTION      = "MySQL DBI DBD, Leverages 'mysql' low-level driver"

            MyError = ::MysqlError

            #
            # returns 'Mysql'
            # 
            # See DBI::TypeUtil#convert for more information.
            #
            def self.driver_name
                "Mysql"
            end

            DBI::TypeUtil.register_conversion(driver_name) do |obj|
                newobj = case obj
                         when ::DBI::Binary
                             obj = obj.to_s.gsub(/\\/) { "\\\\" }
                             obj = obj.to_s.gsub(/'/) { "''" }
                             "'#{obj}'"
                         when ::DateTime
                             "'#{obj.strftime("%Y-%m-%d %H:%M:%S")}'"
                         when ::Time
                             "'#{obj.strftime("%H:%M:%S")}'"
                         when ::Date
                             "'#{obj.strftime("%Y-%m-%d")}'"
                         when ::NilClass
                             "NULL"
                         else
                             obj
                         end

                if newobj.object_id == obj.object_id
                    [newobj, true]
                else
                    [newobj, false]
                end
            end
        end
    end
end

#
# Utility Methods for the MySQL DBD.
#

module DBI::DBD::Mysql::Util
    private

    # Raise exception using information from MysqlError object e.
    # For state value, use SQLSTATE value if mysql-ruby defines
    # sqlstate method, otherwise nil.
    def error(e)
        sqlstate = e.respond_to?("sqlstate") ? e.sqlstate : nil
        raise DBI::DatabaseError.new(e.message, e.errno, sqlstate)
    end

end # module Util

module DBI::DBD::Mysql::Type
    #
    # Custom handling for DATE types in MySQL. See DBI::Type for more
    # information.
    #
    class Date < DBI::Type::Null
        def self.parse(obj)
            obj = super
            return obj unless obj

            case obj.class
            when ::Date
                return obj
            when ::String
                return ::Date.strptime(obj, "%Y-%m-%d")
            else
                return ::Date.parse(obj.to_s)   if obj.respond_to? :to_s
                return ::Date.parse(obj.to_str) if obj.respond_to? :to_str
                return obj
            end
        end
    end
end

require 'dbd/mysql/driver'
require 'dbd/mysql/database'
require 'dbd/mysql/statement'
