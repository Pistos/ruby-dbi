#
# DBD::SQLRelay
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
# $Id: SQLRelay.rb,v 1.1 2006/01/04 02:03:21 francis Exp $
#

require "sqlrelay"


module DBI
module DBD
module SQLRelay


VERSION          = "0.2"
USED_DBD_VERSION = "0.1"


class Driver < DBI::BaseDriver


  def initialize
    super(USED_DBD_VERSION)
  end


  def connect(dbname, user, auth, attr)

    # connect to database
    
    # dbname will have one of these formats:
    # * dbi:SQLRelay:host:port
    # * dbi:SQLRelay:host=xxx;port=xxx;socket=xxx;retrytime=xxx;tries=xxx
    hash = Utils.parse_params(dbname)

    if hash.has_key? "database" then
      # handle the first form
      hash["host"], hash["port"] = hash["database"], hash["host"]
    end

    # set default values if none were supplied
    hash['host']      ||= "localhost"
    hash['port']      ||= "9000"
    hash['socket']    ||= ""
    hash['retrytime'] ||= "0"
    hash['tries']     ||= "1"
    
    # TODO: what happens on connection failure? return nil?
    handle = SQLRConnection.new(hash['host'], hash['port'].to_i, 
      hash['socket'], user, auth, hash['retrytime'].to_i, hash['tries'].to_i)

    return Database.new(handle, attr)
  end

end # class Driver



class Database < DBI::BaseDatabase

  def disconnect
    @handle.endSession
  end


  def ping
    @handle.ping == 1 ? true : false
  end


  def prepare(statement)
    Statement.new(@handle, statement)
  end


  def commit
    $stderr.puts "Warning: Commit ineffective while AutoCommit is on" if @attr['AutoCommit']

    case @handle.commit
    when 0
      # failed
      raise DBI::OperationalError.new("Commit failed")
    when -1
      raise DBI::OperationalError.new("Error occured during commit")
   end
  end


  def rollback
    $stderr.puts "Warning: Rollback ineffective while AutoCommit is on" if @attr['AutoCommit']

    case @handle.rollback
    when 0
      # failed
      raise DBI::OperationalError.new("Rollback failed")
    when -1
      raise DBI::OperationalError.new("Error occured during rollback")
    end
  end


  def []=(attr, value)

    # AutoCommit and sqlrelay_debug are supported by this driver
    case attr
    when 'AutoCommit'
      if value == true
        @handle.autoCommitOn
      else
        @handle.autoCommitOff
      end
    when 'sqlrelay_debug' 
      if value == true
        @handle.debugOn
      else
        @handle.debugOff
      end
    else
      if attr =~ /^sqlrelay_/ or attr != /_/
        # raise and exception for unsupported or improperly formatted options
        raise DBI::NotSupportedError, "Option '#{attr}' not supported"
      else 
        # option for some other driver - quietly ignore
        return  
      end     
    end
    @attr[attr] = value
  end

end # class Database



class Statement < DBI::BaseStatement

  def initialize(handle, stmt)
    super(nil)  # attribs

    @db = handle 
    @handle = SQLRCursor.new(@db)
    @handle.prepareQuery(stmt)
  end


  def bind_param(param, value, attribs)

    # in SQL Relay, bind variable names can be names or numbers and values
    # can be either strings, integers or floats.  Floats come with precision
    # and scale as well.
    if value.kind_of? Float then

      # for float binds, check attribs for precision and scale
      if attribs
        precision = attribs['precision'].to_i
        scale     = attribs['scale'].to_i
      end

      # if either of precision or scale is not passed in, extract them by
      # parsing the value around the decimal point or using defaults
      if precision.nil? or scale.nil?
        pr, sc = value.to_s.split(".")
        precision ||= pr.length || 8
        scale     ||= sc.length || 2
      end

      @handle.inputBind(param.to_s, value.to_f, precision, scale)
    else
      @handle.inputBind(param.to_s, value)
    end
  end


  def execute

    # otherwise execute the already-prepared query, raising an error if it fails
    if @handle.executeQuery == 0 then 
      raise DBI::ProgrammingError.new(@handle.errorMessage)
    end

    # initialize some values
    @row_index = 0
    @row_count = @handle.rowCount
    @affected_rows = @handle.affectedRows

    # clear bind values so the statement can be re-bound and re-executed
    @handle.clearBinds
  end


  def finish
    @handle = nil
  end


  def fetch

    # if we're already at the end of the result set, return nil
    if @row_index >= @row_count 
      return nil
    end

    # otherwise get the current row, increment
    # the row index and return the current row
    row = @handle.getRow(@row_index)
    @row_index += 1
    return row
  end


  def fetch_scroll(direction, offset=1)

    # decide which row to fetch, take into account that the standard behavior
    # of a fetch command is to fetch the row and then skip to the next row
    # of the result set afterward
    fetch_row = case direction
      when DBI::SQL_FETCH_NEXT     then @row_index
      when DBI::SQL_FETCH_PRIOR    then @row_index-2
      when DBI::SQL_FETCH_FIRST    then 0
      when DBI::SQL_FETCH_LAST     then @row_count-1
      when DBI::SQL_FETCH_ABSOLUTE then offset
      when DBI::SQL_FETCH_RELATIVE then @row_index+offset-1
    end
    
    # fetch the row
    row = nil
    if fetch_row > -1 and fetch_row < @row_count
      row = @handle.getRow(fetch_row)
    end

    # set the current row, avoid running past 
    # the end or beginning of the result set
    @row_index = fetch_row + 1
    if @row_index < 0 then
      @row_index = 0
    elsif @row_index > @row_count
      @row_index = @row_count
    end

    return row
  end


  def fetch_many(cnt)

    # fetch the next "cnt" rows and return them
    *rows = []
    index = 0
    while index < cnt and @row_index < @row_count
      *rows[index] = fetch()
      index = index+1
    end
    return *rows
  end


  def fetch_all

    # otherwise, fetch the rest of the rows and return them
    *rows = []
    index = 0
    while @row_index < @row_count
      *rows[index] = fetch()
      index = index+1
    end
    return *rows
  end


  def column_info

    # build a column info hash
    (0...@handle.colCount).collect do |nr|
      {
        'name'      => @handle.getColumnName(nr),
        'type_name' => @handle.getColumnType(nr),
        'precision' => @handle.getColumnLength(nr)
      }
    end
  end


  def rows

    # For DML or DDL queries, row_count is 0 but affected_rows could
    # be non-zero.  For selects, affected_rows will always be zero or
    # equal to row_count (for select into queries, for example).  So,
    # if row_count is 0, send affected_rows.
    @row_count == 0 ? @affected_rows : @row_count
  end

end # class Statement

end # module SQLRelay
end # module DBD
end # module DBI

