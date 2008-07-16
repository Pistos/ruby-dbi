#
# DBD::Mysql
#
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
#

begin
    require 'rubygems'
    gem 'mysql'
rescue LoadError => e
end

require "mysql"
require "thread"   # for Mutex

module DBI
    module DBD
        module Mysql
            module Util

                private

                # Raise exception using information from MysqlError object e.
                # For state value, use SQLSTATE value if mysql-ruby defines
                # sqlstate method, otherwise nil.

                def error(e)
                    sqlstate = e.respond_to?("sqlstate") ? e.sqlstate : nil
                    raise DBI::DatabaseError.new(e.message, e.errno, sqlstate)
                end

            end # module Util
        end
    end
end

require 'dbd/mysql/columninfo'
require 'dbd/mysql/driver'
require 'dbd/mysql/database'
require 'dbd/mysql/statement'

module DBI::DBD::Mysql
    VERSION          = "0.3.3"
    USED_DBD_VERSION = "0.2"

    MyError = ::MysqlError

    def self.driver_name
        "Mysql"
    end

    DBI::TypeUtil.register_conversion(driver_name) do |obj|
        case obj
        when ::Time
            "'#{obj.strftime("%H:%M:%S")}'"
        when ::Date
            "'#{obj.strftime("%m/%d/%Y")}'"
        else
            obj
        end
    end
end # module Mysql
