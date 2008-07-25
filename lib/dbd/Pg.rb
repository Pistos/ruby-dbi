#
# DBD::Pg
#
# Copyright (c) 2001, 2002, 2003 Jim Weirich, Michael Neumann <mneumann@ntecs.de>
# Copyright (c) 2008 Erik Hollensbe, Christopher Maujean
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
    gem 'pg'
rescue Exception => e
end

require 'pg'

# XXX lots of this driver is in require statements below this module definition
module DBI
    module DBD
        module Pg
            VERSION          = "0.3.3"
            USED_DBD_VERSION = "0.2"
            DESCRIPTION      = "PostgreSQL DBI DBD"

            def self.driver_name
                "Pg"
            end

            def self.generate_array(obj)
                # yarr, there be recursion here, and it's probably not a good idea.
                output = "{"
                obj.each do |item|
                    case item
                    when ::Array
                        output += generate_array(item)
                    else
                        generated = DBI::TypeUtil.convert(driver_name, item)
                        if item.kind_of? String
                            # in strings, escapes are doubled and the quotes are different.
                            # this gets *really* ugly and needs to be well-tested
                            generated.gsub!(/\\/) { "\\\\" }
                            generated.gsub!(/(^')|('$)/) { "\"" }
                        end

                        output += generated
                    end
                    output += "," # FIXME technically, delimiters are variable
                end

                output.sub(/,$/, '}')
            end

            def self.quote(value)
                "E'#{ value.gsub(/\\/){ '\\\\' }.gsub(/'/){ '\\\'' } }'"
            end

            def self.parse_type(ftype)
                type = ftype
                pos = ftype.index('(')
                decimal = nil
                size = nil
                array_of_type = nil 

                if pos != nil
                    type = ftype[0..pos-1]
                    size = ftype[pos+1..-2]
                    pos = size.index(',')
                    if pos != nil
                        size, decimal = size.split(',', 2)
                        size = size.to_i
                        decimal = decimal.to_i
                    else
                        size = size.to_i
                    end
                end

                if type =~ /\[\]$/
                    type.sub!(/\[\]$/, '')
                    array_of_type = true
                end

                return {
                    :ftype   => ftype,
                    :type    => type,
                    :size    => size,
                    :decimal => decimal,
                    :array   => array_of_type
                }
            end

            class Driver < DBI::BaseDriver
                def initialize
                    super(USED_DBD_VERSION)
                end

                ## List of datasources for this database.
                def data_sources
                    []
                end

                ## Connect to a database.
                def connect(dbname, user, auth, attr)
                    Database.new(dbname, user, auth, attr)
                end
            end
        end # module Pg
    end # module DBD
end # module DBI

require 'dbd/pg/type'
require 'dbd/pg/database'
require 'dbd/pg/statement'
require 'dbd/pg/tuples'

pg = DBI::DBD::Pg

DBI::TypeUtil.register_conversion(pg.driver_name) do |obj|
    case obj
    when ::DateTime
        pg.quote(obj.strftime("%m/%d/%Y %H:%M:%S.%N"))
    when ::Time, ::Date
        pg.quote(::DateTime.parse(obj.to_s).strftime("%m/%d/%Y %H:%M:%S.%N"))
    when ::Array
        pg.quote(pg.generate_array(obj))
    when ::TrueClass
        "'t'"
    when ::FalseClass
        "'f'"
    when ::NilClass
        "NULL"
    when DBI::DBD::Pg::Type::ByteA
        "E'#{obj.escaped}'"
    else
        obj
    end
end
