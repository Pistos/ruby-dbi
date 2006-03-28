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

require "mysql"
require "thread"   # for Mutex

module DBI
module DBD
module Mysql

VERSION          = "0.3.3"
USED_DBD_VERSION = "0.2"

MyError = ::MysqlError

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

class Driver < DBI::BaseDriver
  include Util

  def initialize
    super(USED_DBD_VERSION)
  end

  def connect(dbname, user, auth, attr)
    # connect to database
    hash = Utils.parse_params(dbname)

    #if hash['database'].nil? 
    #  raise DBI::InterfaceError, "must specify database"
    #end

    hash['host'] ||= 'localhost'

    # these two connection parameters should be passed as numbers
    hash['port'] = hash['port'].to_i unless hash['port'].nil?
    hash['flag'] = hash['flag'].to_i unless hash['flag'].nil?

    handle = ::Mysql.init

    # Look for options in connect string that must be handled
    # through mysql_options() before connecting
    !hash['mysql_read_default_file'].nil? and
      handle.options(::Mysql::READ_DEFAULT_FILE,
                     hash['mysql_read_default_file'])
    !hash['mysql_read_default_group'].nil? and
      handle.options(::Mysql::READ_DEFAULT_GROUP,
                     hash['mysql_read_default_group'])

    handle.connect(hash['host'], user, auth, hash['database'], hash['port'], hash['socket'], hash['flag'])
    #handle.select_db(hash['database'])

    return Database.new(handle, attr)
  rescue MyError => err
    error(err)
  end

  def data_sources
    handle = ::Mysql.new
    res = handle.list_dbs.collect {|db| "dbi:Mysql:database=#{db}" }
    handle.close
    return res
  rescue MyError => err
    error(err)
  end

  # Driver-specific functions ------------------------------------------------

  public

  def __createdb(db, host, user, password, port=nil, sock=nil, flag=nil)
    handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
    begin
      handle.create_db(db)
    ensure
      handle.close if handle
    end
  end

  def __dropdb(db, host, user, password, port=nil, sock=nil, flag=nil)
    handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
    begin
      handle.drop_db(db)
    ensure
      handle.close if handle
    end
  end

  def __shutdown(host, user, password, port=nil, sock=nil, flag=nil)
    handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
    begin
      handle.shutdown
    ensure
      handle.close if handle
    end
  end

  def __reload(host, user, password, port=nil, sock=nil, flag=nil)
    handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
    begin
      handle.reload
    ensure
      handle.close if handle
    end
  end

end # class Driver

class Database < DBI::BaseDatabase
  include Util
  include SQL::BasicBind

  # Eli Green:
  #   The hope is that we don't ever need to just assume the default values. However,
  #   in some cases (notably floats and doubles), I have seen "show fields from table"
  #   return absolutely zero information about size and precision. Sigh.
  #   I probably should have made a struct to store this info in ... but I didn't.
  MYSQL_to_XOPEN = {
    "TINYINT"    => [SQL_TINYINT, 1, nil],
    "SMALLINT"   => [SQL_SMALLINT, 6, nil],
    "MEDIUMINT"  => [SQL_SMALLINT, 6, nil],
    "INT"        => [SQL_INTEGER, 11, nil],
    "INTEGER"    => [SQL_INTEGER, 11, nil],
    "BIGINT"     => [SQL_BIGINT, 25, nil],
    "INT24"      => [SQL_BIGINT, 25, nil],
    "REAL"       => [SQL_REAL, 12, nil],
    "FLOAT"      => [SQL_FLOAT, 12, nil],
    "DECIMAL"    => [SQL_DECIMAL, 12, nil],
    "NUMERIC"    => [SQL_NUMERIC, 12, nil],
    "DOUBLE"     => [SQL_DOUBLE, 22, nil],
    "CHAR"       => [SQL_CHAR, 1, nil],
    "VARCHAR"    => [SQL_VARCHAR, 255, nil],
    "DATE"       => [SQL_DATE, 10, nil],
    "TIME"       => [SQL_TIME, 8, nil],
    "TIMESTAMP"  => [SQL_TIMESTAMP, 19, nil],
    "DATETIME"   => [SQL_TIMESTAMP, 19, nil],
    "TINYBLOB"   => [SQL_BINARY, 255, nil],
    "BLOB"       => [SQL_VARBINARY, 16277215, nil],
    "MEDIUMBLOB" => [SQL_VARBINARY, 2147483657, nil],
    "LONGBLOB"   => [SQL_LONGVARBINARY, 2147483657, nil],
    "TINYTEXT"   => [SQL_VARCHAR, 255, nil],
    "TEXT"       => [SQL_LONGVARCHAR, 65535, nil],
    "MEDIUMTEXT" => [SQL_LONGVARCHAR, 16277215, nil],
    "ENUM"       => [SQL_CHAR, 255, nil],
    "SET"        => [SQL_CHAR, 255, nil],
    nil          => [SQL_OTHER, nil, nil]
  }

  TYPE_MAP = {}
  MysqlField.constants.grep(/^TYPE_/).each do |const|
    value = MysqlField.const_get(const)
    case const
    when 'TYPE_TINY', 'TYPE_INT24', 'TYPE_SHORT', 'TYPE_LONG', 'TYPE_LONGLONG'
      TYPE_MAP[value] = :as_int
    when 'TYPE_FLOAT'
      TYPE_MAP[value] = :as_float
    when 'TYPE_DATE'
      TYPE_MAP[value] = :as_date
    when 'TYPE_TIME'
      TYPE_MAP[value] = :as_time
    when 'TYPE_DATETIME'
      TYPE_MAP[value] = :as_timestamp
    else
      TYPE_MAP[value] = :as_str
    end
  end

  def initialize(handle, attr)
    super
    # check server version to determine transaction capability
    ver_str = @handle.get_server_info
    major, minor, teeny = ver_str.split(".")
    teeny.sub!(/\D*$/, "")	# strip any non-numeric suffix if present
    server_version = major.to_i*10000 + minor.to_i*100 + teeny.to_i
    # It's not until 3.23.17 that SET AUTOCOMMIT,
    # BEGIN, COMMIT, and ROLLBACK all are available
    @have_transactions = (server_version >= 32317)
    # assume the connection begins in AutoCommit mode
    @attr['AutoCommit'] = true
    @mutex = Mutex.new 
  end

  def disconnect
    self.rollback unless @attr['AutoCommit']
    @handle.close
  rescue MyError => err
    error(err)
  end

  def ping
    begin
      @handle.ping
      return true
    rescue MyError
      return false
    end
  end

  def tables
    @handle.list_tables
  rescue MyError => err
    error(err)
  end

  # Eli Green (fixed up by Michael Neumann)
  def columns(table)
    dbh = DBI::DatabaseHandle.new(self)
    uniques = []
    dbh.execute("SHOW INDEX FROM #{table}") do |sth|
      sth.each do |row|
        uniques << row[4] if row[1] == "0"
      end
    end  

    ret = nil
    dbh.execute("SHOW FIELDS FROM #{table}") do |sth|
      ret = sth.collect do |row|
        name, type, nullable, key, default, extra = row
        #type = row[1]
        #size = type[type.index('(')+1..type.index(')')-1]
        #size = 0
        #type = type[0..type.index('(')-1]

        sqltype, type, size, decimal = mysql_type_info(row[1])
        col = Hash.new
        col['name']           = name
        col['sql_type']       = sqltype
        col['type_name']      = type
        col['nullable']       = nullable == "YES"
        col['indexed']        = key != ""
        col['primary']        = key == "PRI"
        col['unique']         = uniques.index(name) != nil
        col['precision']      = size
        col['scale']          = decimal
        col['default']        = row[4]
        col
      end # collect
    end # execute
   
    ret
  end

  def do(stmt, *bindvars)
    sql = bind(self, stmt, bindvars)
    @mutex.synchronize { 
      @handle.query_with_result = false
      @handle.query(sql)
      @handle.affected_rows     # return value
    }
  rescue MyError => err
    error(err)
  end
 

  def prepare(statement)
    Statement.new(self, @handle, statement, @mutex)
  end

  def commit
    if @have_transactions
      self.do("COMMIT")
    else
      raise NotSupportedError
    end
  rescue MyError => err
    error(err)
  end

  def rollback
    if @have_transactions
      self.do("ROLLBACK")
    else
      raise NotSupportedError
    end
  rescue MyError => err
    error(err)
  end


  def quote(value)
    case value
    when String
      "'#{@handle.quote(value)}'"
    when DBI::Binary
      "'#{@handle.quote(value.to_s)}'"
    else
      super
    end
  end

  def []=(attr, value)
    case attr
    when 'AutoCommit'
      if @have_transactions
        self.do("SET AUTOCOMMIT=" + (value ? "1" : "0"))
      else
        raise NotSupportedError
      end
    else
      raise NotSupportedError
    end
    @attr[attr] = value
  end

  private # -------------------------------------------------

  # Eli Green
  def mysql_type_info(typedef)
    sql_type, type, size, decimal = nil, nil, nil, nil

    pos = typedef.index('(')
    if not pos.nil?
      type = typedef[0..pos-1]
      size = typedef[pos+1..-2]
      pos = size.index(',')
      if not pos.nil?
        size, decimal = size.split(',', 2)
        decimal = decimal.to_i
      end
      size = size.to_i
    else
      type = typedef
    end

    type_info = MYSQL_to_XOPEN[type.upcase] || MYSQL_to_XOPEN[nil]
    sqltype = type_info[0]
    if size.nil? then size = type_info[1] end
    if decimal.nil? then decimal = type_info[2] end
    return sqltype, type, size, decimal
  end

  
  # Driver-specific functions ------------------------------------------------

  public

  def __createdb(db)
    @handle.create_db(db)
  end

  def __dropdb(db)
    @handle.drop_db(db)
  end

  def __shutdown
    @handle.shutdown
  end

  def __reload
    @handle.reload
  end

  def __insert_id
    @handle.insert_id
  end


end # class Database


class Statement < DBI::BaseStatement
  include Util
  #include SQL::BasicBind

  def initialize(parent, handle, statement, mutex)
    super(nil)

    @parent, @handle, @mutex = parent, handle, mutex
    @params = []

    @prep_stmt = DBI::SQL::PreparedStatement.new(@parent, statement)
  end

  def bind_param(param, value, attribs)
    raise InterfaceError, "only ? parameters supported" unless param.is_a? Fixnum
    @params[param-1] = value 
  end

  def execute
    sql = @prep_stmt.bind(@params)
    @mutex.synchronize {
      @handle.query_with_result = true
      @res_handle = @handle.query(sql)
      @column_info = self.column_info
      @coerce = DBI::SQL::BasicQuote::Coerce.new
      @current_row = 0
      @rows = @handle.affected_rows
    }
  rescue MyError => err
    error(err)
  end

  def finish
    @res_handle.free if @res_handle
  rescue MyError => err
    error(err)
  end

  def fill_array(rowdata)
    return nil if rowdata.nil?
    row = []
    rowdata.each_with_index { |value, index|
      type = @column_info[index]['_type']
      type_symbol = Database::TYPE_MAP[type] || :as_str
      row[index] = @coerce.coerce(type_symbol, value)
    }
    row
  end

  def fetch
    @current_row += 1
    fill_array(@res_handle.fetch_row)
  rescue MyError => err
    error(err)
  end

  def fetch_scroll(direction, offset)
      case direction
      when SQL_FETCH_NEXT
        @current_row += 1
        fill_array(@res_handle.fetch_row)
      when SQL_FETCH_PRIOR
        @res_handle.data_seek(@current_row - 1)
        fill_array(@res_handle.fetch_row)
      when SQL_FETCH_FIRST
        @current_row = 1
        @res_handle.data_seek(@current_row - 1)
        fill_array(@res_handle.fetch_row)
      when SQL_FETCH_LAST
        @current_row = @res_handle.num_rows
        @res_handle.data_seek(@current_row - 1)
        fill_array(@res_handle.fetch_row)
      when SQL_FETCH_ABSOLUTE
        @current_row = offset + 1
        @res_handle.data_seek(@current_row - 1)
        fill_array(@res_handle.fetch_row)
      when SQL_FETCH_RELATIVE
        @current_row += offset + 1
        @res_handle.data_seek(@current_row - 1)
        fill_array(@res_handle.fetch_row)
      else
        raise NotSupportedError
      end
    #end
  end

  def column_info
    retval = []

    return [] if @res_handle.nil?

    @res_handle.fetch_fields.each {|col| 
      retval << {'name' => col.name, 'precision' => col.length - col.decimals, 'scale' => col.decimals,
      '_type' => col.type, '_length' => col.length, '_max_length' => col.max_length, '_flags' => col.flags }
    }
    retval
  rescue MyError => err
    error(err)
  end

  def rows
    @rows
  end

=begin
  def []=(attr, value)
    case attr
    when 'mysql_use_result'
      @attr['mysql_store_result'] = ! value
      @attr['mysql_use_result']   = value
    when 'mysql_store_result'
      @attr['mysql_use_result']   = ! value
      @attr['mysql_store_result'] = value
    else
      raise NotSupportedError
    end
  end
=end

end # class Statement


end # module Mysql
end # module DBD
end # module DBI

