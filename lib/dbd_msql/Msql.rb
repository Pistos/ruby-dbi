#
# DBD::Msql
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
# $Id: Msql.rb,v 1.1 2006/01/04 02:03:19 francis Exp $
#

require "msql"

module DBI
module DBD
module Msql

VERSION          = "0.1"
USED_DBD_VERSION = "0.1"

MyError = ::MsqlError

class Driver < DBI::BaseDriver

  def initialize
    super(USED_DBD_VERSION)
  end

  def connect(dbname, user, auth, attr)
    # connect to database
    hash = Utils.parse_params(dbname)

    if hash['database'].nil? 
      raise DBI::InterfaceError, "must specify database"
    end

    #hash['host'] ||= 'localhost'

    handle = ::Msql.connect(hash['host'], hash['database'])
    return Database.new(handle, attr)
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def data_sources
    handle = ::Msql.connect
    res = handle.list_dbs.fetch_all_rows.collect {|db| "dbi:Msql:database=#{db[0]}" }
    handle.close
    return res
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

end # class Driver

class Database < DBI::BaseDatabase
  include SQL::BasicBind

  def disconnect
    @handle.close
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def ping
    super
    #begin
    #  @handle.ping
    #  return true
    #rescue MyError
    #  return false
    #end
  end

  def tables
    @handle.list_tables.fetch_all_rows.collect {|tab| tab[0] }
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end


  def do(stmt, *bindvars)
    sql = bind(self, stmt, bindvars)
    @handle.query(sql)
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end
 
  def prepare(statement)
    Statement.new(@handle, statement)
  end

  # TODO: Raise Error
  def commit
  end

  # TODO: Raise Error
  def rollback
  end

end # class Database


class Statement < DBI::BaseStatement
  include SQL::BasicBind
  include SQL::BasicQuote

  def initialize(handle, statement)
    @handle = handle
    @statement = statement
    @params = []
  end

  def bind_param(param, value, attribs)
    raise InterfaceError, "only ? parameters supported" unless param.is_a? Fixnum
    @params[param-1] = value 
  end

  def execute
    sql = bind(self, @statement, @params)
    @rows = @handle.query(sql)
    @res_handle = @handle.get_result # only SELECT ?
    @row_pos = 0
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def finish
    @res_handle = nil
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def fetch
    res = @res_handle.fetch_row
    if res.nil?
      @row_pos = nil 
    else
      @row_pos += 1 unless @row_pos.nil?
    end
    res
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def fetch_scroll( direction, offset )
    case direction
    when DBI::SQL_FETCH_NEXT
      fetch
    when DBI::SQL_FETCH_PRIOR
      if @row_pos.nil?
        @row_pos = @res_handle.num_rows-1
        @res_handle.data_seek(@row_pos)
        fetch
      elsif @row_pos == 0
        nil
      else
        @row_pos -= 1
        @res_handle.data_seek(@row_pos)
        fetch
      end 
    when DBI::SQL_FETCH_FIRST
      @row_pos = 0
      @res_handle.data_seek(@row_pos)
      fetch
    when DBI::SQL_FETCH_LAST
      @res_handle.data_seek(@res_handle.num_rows-1)
      @row_pos = nil 
      @res_handle.fetch_row
    when DBI::SQL_FETCH_ABSOLUTE
      @row_pos = offset
      @res_handle.data_seek(@row_pos)
      fetch
    when DBI::SQL_FETCH_RELATIVE
      if @row_pos.nil?
        @row_pos = @res_handle.num_rows
      end
      @row_pos += offset 
      @res_handle.data_seek(@row_pos)
      fetch
    end
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end


  def column_info
    retval = []

    return [] if @res_handle.nil?

    @res_handle.each_field {|col| 
      retval << {'name' => col.name }
    }
    retval
  rescue MyError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def rows
    @rows
  end

end # class Statement


end # module Msql
end # module DBD
end # module DBI

