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


# The MySQL-specific column_info method defines some MySQL-specific
# members of the column info attribute array. Extend ColumnInfo by
# adding methods so that info["mysql_x"], for each MySQL-specific x, also
# has get and set info.mysql_x and info.mysql_x= methods.

class ColumnInfo

  # Get the column's MySQL type code
  def mysql_type
     self['mysql_type']
  end
   
  # Set the column's MySQL type code
  def mysql_type=(val)
     self['mysql_type'] = val
  end

  # Get the column's MySQL type name
  def mysql_type_name
     self['mysql_type_name']
  end
   
  # Set the column's MySQL type name
  def mysql_type_name=(val)
     self['mysql_type_name'] = val
  end

  # Get the column's MySQL length
  def mysql_length
     self['mysql_length']
  end
   
  # Set the column's MySQL length
  def mysql_length=(val)
     self['mysql_length'] = val
  end

  # Get the column's MySQL max length
  def mysql_max_length
     self['mysql_max_length']
  end
   
  # Set the column's MySQL max length
  def mysql_max_length=(val)
     self['mysql_max_length'] = val
  end

  # Get the column's MySQL flags
  def mysql_flags
     self['mysql_flags']
  end
   
  # Set the column's MySQL flags
  def mysql_flags=(val)
     self['mysql_flags'] = val
  end

end # class ColumnInfo

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

  def default_user
    ['', nil]
  end

  def connect(dbname, user, auth, attr)
    # connect to database server
    hash = Utils.parse_params(dbname)

    hash['host'] ||= 'localhost'

    # these two connection parameters should be passed as numbers
    hash['port'] = hash['port'].to_i unless hash['port'].nil?
    hash['flag'] = hash['flag'].nil? ? 0 : hash['flag'] = hash['flag'].to_i

    handle = ::Mysql.init

    # Look for options in connect string to be handled
    # through mysql_options() before connecting
    !hash['mysql_read_default_file'].nil? and
      handle.options(::Mysql::READ_DEFAULT_FILE,
                     hash['mysql_read_default_file'])
    !hash['mysql_read_default_group'].nil? and
      handle.options(::Mysql::READ_DEFAULT_GROUP,
                     hash['mysql_read_default_group'])
    # The following options can be handled either using mysql_options()
    # or in the flag argument to connect().
    hash['mysql_compression'].to_i != 0 and
      handle.options(::Mysql::OPT_COMPRESS, nil)
    hash['mysql_local_infile'].to_i != 0 and
      handle.options(::Mysql::OPT_LOCAL_INFILE, true)

    # Look for options to be handled in the flags argument to connect()
    if !hash['mysql_client_found_rows'].nil?
      if hash['mysql_client_found_rows'].to_i != 0
        hash['flag'] |= ::Mysql::CLIENT_FOUND_ROWS
      else
        hash['flag'] &= ~::Mysql::CLIENT_FOUND_ROWS
      end
    end

    handle.connect(hash['host'], user, auth, hash['database'], hash['port'], hash['socket'], hash['flag'])

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
  #   The hope is that we don't ever need to just assume the default values.
  #   However, in some cases (notably floats and doubles), I have seen
  #   "show fields from table" return absolutely zero information about size
  #   and precision. Sigh. I probably should have made a struct to store
  #   this info in ... but I didn't.
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
    "BLOB"       => [SQL_VARBINARY, 65535, nil],
    "MEDIUMBLOB" => [SQL_VARBINARY, 16277215, nil],
    "LONGBLOB"   => [SQL_LONGVARBINARY, 2147483657, nil],
    "TINYTEXT"   => [SQL_VARCHAR, 255, nil],
    "TEXT"       => [SQL_LONGVARCHAR, 65535, nil],
    "MEDIUMTEXT" => [SQL_LONGVARCHAR, 16277215, nil],
    "LONGTEXT"   => [SQL_LONGVARCHAR, 2147483657, nil],
    "ENUM"       => [SQL_CHAR, 255, nil],
    "SET"        => [SQL_CHAR, 255, nil],
    "BIT"        => [SQL_BIT, 8, nil],
    nil          => [SQL_OTHER, nil, nil]
  }

  # Map MySQL numeric type codes to:
  # - (uppercase) MySQL type names
  # - coercion method

  TYPE_MAP = {}
  MysqlField.constants.grep(/^TYPE_/).each do |const|
    mysql_type = MysqlField.const_get(const)  # numeric type code
    coercion_method = :as_str                 # default coercion method
    case const
    when 'TYPE_TINY':         mysql_type_name = 'TINYINT'
                              coercion_method = :as_int
    when 'TYPE_SHORT':        mysql_type_name = 'SMALLINT'
                              coercion_method = :as_int
    when 'TYPE_INT24':        mysql_type_name = 'MEDIUMINT'
                              coercion_method = :as_int
    when 'TYPE_LONG':         mysql_type_name = 'INT'
                              coercion_method = :as_int
    when 'TYPE_LONGLONG':     mysql_type_name = 'BIGINT'
                              coercion_method = :as_int
    when 'TYPE_FLOAT':        mysql_type_name = 'FLOAT'
                              coercion_method = :as_float
    when 'TYPE_DOUBLE':       mysql_type_name = 'DOUBLE'
                              coercion_method = :as_float
    when 'TYPE_VAR_STRING',
         'TYPE_STRING':       mysql_type_name = 'VARCHAR'    # questionable?
                              coercion_method = :as_str
    when 'TYPE_DATE':         mysql_type_name = 'DATE'
                              coercion_method = :as_date
    when 'TYPE_TIME':         mysql_type_name = 'TIME'
                              coercion_method = :as_time
    when 'TYPE_DATETIME':     mysql_type_name = 'DATETIME'
                              coercion_method = :as_timestamp
    when 'TYPE_CHAR':         mysql_type_name = 'TINYINT'    # questionable?
    when 'TYPE_TINY_BLOB':    mysql_type_name = 'TINYBLOB'   # questionable?
    when 'TYPE_MEDIUM_BLOB':  mysql_type_name = 'MEDIUMBLOB' # questionable?
    when 'TYPE_LONG_BLOB':    mysql_type_name = 'LONGBLOB'   # questionable?
    when 'TYPE_GEOMETRY':     mysql_type_name = 'BLOB'       # questionable?
    when 'TYPE_YEAR',
         'TYPE_TIMESTAMP',
         'TYPE_DECIMAL',                                     # questionable?
         'TYPE_BLOB',                                        # questionable?
         'TYPE_ENUM',
         'TYPE_SET',
         'TYPE_BIT',
         'TYPE_NULL':         mysql_type_name = const.sub(/^TYPE_/, '')
    else
                              mysql_type_name = 'UNKNOWN'
    end
    TYPE_MAP[mysql_type] = [mysql_type_name, coercion_method]
  end
  TYPE_MAP[nil] = ['UNKNOWN', :as_str]

  def initialize(handle, attr)
    super
    # check server version to determine transaction capability
    ver_str = @handle.get_server_info
    major, minor, teeny = ver_str.split(".")
    teeny.sub!(/\D*$/, "")  # strip any non-numeric suffix if present
    server_version = major.to_i*10000 + minor.to_i*100 + teeny.to_i
    # It's not until 3.23.17 that SET AUTOCOMMIT,
    # BEGIN, COMMIT, and ROLLBACK all are available
    @have_transactions = (server_version >= 32317)
    # assume that the connection begins in AutoCommit mode
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
  # Parse column type string (from SHOW FIELDS) to extract type info:
  # - sqltype: XOPEN type number
  # - type: MySQL type name
  # - size: column length (or precision)
  # - decimal: number of decimals (scale)
  def mysql_type_info(typedef)
    sqltype, type, size, decimal = nil, nil, nil, nil

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
      type = @column_info[index]['mysql_type']
      type_symbol = Database::TYPE_MAP[type][1] || :as_str
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

    unique_key_flag = MysqlField.const_get(:UNIQUE_KEY_FLAG)
    multiple_key_flag = MysqlField.const_get(:MULTIPLE_KEY_FLAG)
    indexed = (unique_key_flag | multiple_key_flag)

    # Note: Cannot get 'default' column attribute because MysqlField.def
    # is set only by mysql_list_fields()

    @res_handle.fetch_fields.each {|col| 
      mysql_type_name = Database::TYPE_MAP[col.type][0]
      xopen_info = Database::MYSQL_to_XOPEN[mysql_type_name] ||
                   Database::MYSQL_to_XOPEN[nil]
      sql_type = xopen_info[0]
      type_name = DBI::SQL_TYPE_NAMES[sql_type]

      retval << {
                  # Standard Ruby DBI column attributes
                  'name'        => col.name,
                  'sql_type'    => sql_type,
                  'type_name'   => type_name,
                  'precision'   => col.length,
                  'scale'       => col.decimals,
                  'nullable'    => !col.is_not_null?,
                  'indexed'     => ((col.flags & indexed) != 0) ||
                                   col.is_pri_key?,
                  'primary'     => col.is_pri_key?,
                  'unique'      => ((col.flags & unique_key_flag) != 0) ||
                                   col.is_pri_key?,
                  # MySQL-specific attributes (signified by leading "mysql_")
                  'mysql_type'       => col.type,
                  'mysql_type_name'  => mysql_type_name,
                  'mysql_length'     => col.length,
                  'mysql_max_length' => col.max_length,
                  'mysql_flags'      => col.flags
                }
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
