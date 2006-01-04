#
# DBD::FrontBase
#
# Copyright (c) 2003, 2004 Cail Borrell <cail@frontbase.com>
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
#

require 'frontbase'

module DBI
  module DBD
    module FrontBase
      
      VERSION          = "0.5.2"
      USED_DBD_VERSION = "0.2"
      
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
      
      ################################################################
      class Database < DBI::BaseDatabase

        # type map ---------------------------------------------------
        
        FRONTBASE_to_XOPEN = {
           "BOOLEAN"                  => [SQL_TINYINT, 1, nil],
           "TINYINT"                  => [SQL_TINYINT, 7, nil],
           "SMALLINT"                 => [SQL_SMALLINT, 15, nil],
           "INTEGER"                  => [SQL_INTEGER, 31, nil],
           "PRIMARY KEY"              => [SQL_INTEGER, 31, nil],
           "LONGINT"                  => [SQL_BIGINT, 63, nil],
           "REAL"                     => [SQL_REAL, 19, nil],
           "FLOAT"                    => [SQL_FLOAT, 19, nil],
           "DECIMAL"                  => [SQL_DECIMAL, 38, nil],
           "NUMERIC"                  => [SQL_NUMERIC, 19, nil],
           "DOUBLE"                   => [SQL_DOUBLE, 38, nil],
           "CHAR"                     => [SQL_CHAR, 2147483646, nil],
           "VARCHAR"                  => [SQL_VARCHAR, 2147483646, nil],
           "DATE"                     => [SQL_DATE, 6, nil],
           "TIME"                     => [SQL_TIME, 6, nil],
           "TIME WITH TIME ZONE"      => [SQL_TIME, 6, nil],
           "TIMESTAMP"                => [SQL_TIMESTAMP, 6, nil],
           "TIMESTAMP WITH TIME ZONE" => [SQL_TIMESTAMP, 6, nil],
           "BLOB"                     => [SQL_BLOB, 2147483646, nil],
           "CLOB"                     => [SQL_CLOB, 2147483646, nil],
           "BIT"                      => [SQL_BINARY, 2147483646, nil],
           "BIT VARYING"              => [SQL_VARBINARY, 2147483646, nil],
           "BYTE"                     => [SQL_BINARY, 2147483646, nil],
           "BYTE VARYING"             => [SQL_VARBINARY, 2147483646, nil],
           nil                        => [SQL_OTHER, nil, nil]
        }
        
        def initialize(dbname, user, auth, attr)
           hash = Utils.parse_params(dbname)

           if hash['dbname'].nil? and hash['database'].nil?
              raise DBI::InterfaceError, "must specify database"
           end

           hash['dbpasswd'] ||= ''
           hash['port'] = hash['port'].to_i unless hash['port'].nil? 

           @connection = FBSQL_Connect.connect(hash['host'], hash['port'], hash['dbname'] || hash['database'], user, auth, hash['dbpasswd']);

           @exec_method = :exec
           @query_method = :query
           @attr = attr
           self['AutoCommit'] = true

        rescue FBError => err
           raise DBI::OperationalError.new(err.message)
        end
        
        # DBD Protocol -----------------------------------------------

        def disconnect
           @connection.close
        end
        
        def ping
           answer = __query("VALUES(SERVER_NAME)")
           if answer
              return answer.num_rows == 1
           else
              return false
           end
        rescue FBError
           return false
        ensure
           answer.clear if answer
        end

        def tables
           stmt = execute("SELECT \"TABLE_NAME\" FROM \"INFORMATION_SCHEMA\".\"SCHEMATA\" AS T0, \"INFORMATION_SCHEMA\".\"TABLES\" AS T1 WHERE T0.\"SCHEMA_PK\" = T1.\"SCHEMA_PK\" AND T0.\"SCHEMA_NAME\" <> 'DEFINITION_SCHEMA' AND T1.\"TABLE_TYPE\" IN ('BASE TABLE', 'VIEW') AND T0.\"SCHEMA_NAME\" = CURRENT_SCHEMA ORDER BY \"TABLE_NAME\"")
           res = stmt.fetch_all.collect {|row| row[0]} 
           stmt.finish
           res
        end

        def columns(table)
           sql = "SELECT T3.\"COLUMN_NAME\" AS \"NAME\", CASE WHEN T4.\"DATA_TYPE\" = 'BOOLEAN' THEN -6 WHEN T4.\"DATA_TYPE\" = 'TINYINT' THEN -6 WHEN T4.\"DATA_TYPE\" = 'SMALLINT' THEN 5 WHEN T4.\"DATA_TYPE\" = 'INTEGER' THEN 4 WHEN T4.\"DATA_TYPE\" = 'PRIMARY KEY' THEN 4 WHEN T4.\"DATA_TYPE\" = 'LONGINT' THEN -5 WHEN T4.\"DATA_TYPE\" = 'REAL' THEN 7 WHEN T4.\"DATA_TYPE\" = 'FLOAT' THEN 6 WHEN T4.\"DATA_TYPE\" = 'DECIMAL' THEN 3 WHEN T4.\"DATA_TYPE\" = 'NUMERIC' THEN 2 WHEN T4.\"DATA_TYPE\" = 'DOUBLE PRECISION' THEN 8 WHEN T4.\"DATA_TYPE\" = 'CHARACTER' THEN 1 WHEN T4.\"DATA_TYPE\" = 'CHARACTER VARYING' THEN 2 WHEN T4.\"DATA_TYPE\" = 'DATE' THEN 9 WHEN T4.\"DATA_TYPE\" = 'TIME' THEN 10 WHEN T4.\"DATA_TYPE\" = 'TIME WITH TIME ZONE' THEN 10 WHEN T4.\"DATA_TYPE\" = 'TIMESTAMP' THEN 11 WHEN T4.\"DATA_TYPE\" = 'TIMESTAMP WITH TIME ZONE' THEN 11 WHEN T4.\"DATA_TYPE\" = 'BLOB' THEN -10 WHEN T4.\"DATA_TYPE\" = 'CLOB' THEN -11 WHEN T4.\"DATA_TYPE\" = 'BIT' THEN -2 WHEN T4.\"DATA_TYPE\" = 'BIT VARYING' THEN -3 WHEN T4.\"DATA_TYPE\" = 'BYTE' THEN -2 WHEN T4.\"DATA_TYPE\" = 'BYTE VARYING' THEN -3 ELSE 100 END AS \"SQL_TYPE\", T4.\"DATA_TYPE\" AS \"TYPE_NAME\", CASE WHEN T3.\"IS_NULLABLE\" = 'YES' THEN 'true' ELSE 'false' END AS \"NULLABLE\", CAST (NULL AS BOOLEAN) AS \"INDEXED\", CASE WHEN (SELECT T6.\"CONSTRAINT_TYPE\" FROM \"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" T5, \"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\" T6 WHERE T5.\"CONSTRAINT_NAME_PK\" = T6.\"CONSTRAINT_NAME_PK\" AND T5.\"COLUMN_PK\" = T3.\"COLUMN_PK\" AND T6.\"CONSTRAINT_TYPE\" = 'PRIMARY KEY') IS NULL THEN 'false' ELSE 'true' END AS \"PRIMARY\", CASE WHEN (SELECT T6.\"CONSTRAINT_TYPE\" FROM \"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" T5, \"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\" T6 WHERE T5.\"CONSTRAINT_NAME_PK\" = T6.\"CONSTRAINT_NAME_PK\" AND T5.\"COLUMN_PK\" = T3.\"COLUMN_PK\" AND T6.\"CONSTRAINT_TYPE\" = 'UNIQUE') IS NULL THEN 'false' ELSE 'true' END AS \"UNIQUE\", CASE WHEN T4.NUMERIC_PRECISION IS NULL THEN T4.\"CHARACTER_MAXIMUM_LENGTH\" ELSE T4.\"NUMERIC_PRECISION\" END AS \"PRECISION\", T4.\"NUMERIC_SCALE\" AS \"SCALE\", T3.\"COLUMN_DEFAULT\" AS \"DEFAULT\" FROM \"INFORMATION_SCHEMA\".\"CATALOGS\" T0, \"INFORMATION_SCHEMA\".\"SCHEMATA\" T1, \"INFORMATION_SCHEMA\".\"TABLES\" T2, \"INFORMATION_SCHEMA\".\"COLUMNS\" T3, \"INFORMATION_SCHEMA\".\"DATA_TYPE_DESCRIPTOR\" T4 WHERE T0.\"CATALOG_PK\" = T1.\"CATALOG_PK\" AND T1.\"SCHEMA_PK\" = T2.\"SCHEMA_PK\" AND T2.\"TABLE_PK\" = T3.\"TABLE_PK\" AND T3.\"COLUMN_PK\" = T4.\"COLUMN_NAME_PK\" AND T1.\"SCHEMA_NAME\" LIKE CURRENT_SCHEMA AND T2.\"TABLE_NAME\" LIKE '#{table}';"

           stmt = execute(sql)
           res = stmt.fetch_all 
           stmt.finish
           res
        end
 
        def prepare(statement)
           Statement.new(self, statement)
        end
        
        def [](attr)
           @attr[attr]
        end

        def []=(attr, value)
           case attr
           when 'AutoCommit'
              @connection.autocommit(value ? 1 : 0)
	     end
           @attr[attr] = value
        end

        def commit
           @connection.commit
        end

        def rollback
           @connection.rollback
        end

        # Other Public Methods ---------------------------------------

        def __exec(sql)
           @connection.send(@exec_method, sql)
        end

        def __query(sql)
           @connection.send(@query_method, sql)
        end
        
        def __create_blob(data)
           @connection.create_blob(data)
        end

      end # Database

      ################################################################
      class Statement < DBI::BaseStatement
        
        def initialize(db, sql)
           @db  = db
           @coerce = DBI::SQL::BasicQuote::Coerce.new
           @prep_sql = DBI::SQL::PreparedStatement.new(@db, sql)
           @result = nil
           @bindvars = []
        end
        
        def bind_param(index, value, options)
           @bindvars[index-1] = value
        end

        def execute
           # replace DBI::Binary object with blob handle 
           @bindvars.collect! do |var|
              if var.is_a? DBI::Binary then
                 blob = @db.__create_blob(var.to_s)
                 DBI::Binary.new(blob.handle)
              else
                 var
              end
           end

           boundsql = @prep_sql.bind(@bindvars)

           fb_result = @db.__query(boundsql)
           @result = Tuples.new(@db, fb_result)

        rescue FBError, RuntimeError => err
           raise DBI::ProgrammingError.new(err.message)
        end
        
        def fetch
           @result.fetchrow
        end

        def fetch_scroll(direction, offset)
           @result.fetch_scroll(direction, offset)
        end

        def finish
           @result.finish if @result
           @result = nil
           @db = nil
        end
        
        # returns result-set column informations
        def column_info
           @result.column_info
        end
        
        # Return the row processed count (or nil if RPC not available)
        def rows
           if @result
              @result.rows_affected
           else
              nil
           end
        end

        def [](attr)
           case attr
           when 'fb_row_count'
              if @result
                 @result.row_count
              else
                 nil
              end
           else
              @attr[attr]
           end
        end

      end # Statement
      
      ################################################################
      class Tuples

        def initialize(db,fb_result)
           @db = db
           @fb_result = fb_result
           @index = -1
           @result = @fb_result.result
           @row = Array.new
           @coerce = DBI::SQL::BasicQuote::Coerce.new
        end

        def column_info
           @fb_result.columns.collect do |str| {'name'=>str} end
        end

        def fetchrow
           @index += 1
           
           if @index >= @result.size
              @result = @fb_result.result
              @index = 0
           end
           
           if @result.size > 0
              fill_array(@result[@index])
              @row
           else
              nil           
           end
        end

        def fetch_scroll(direction, offset)
           # Exact semantics aren't too closely defined.  I attempted to follow the DBI:Mysql example.
           case direction
           when SQL_FETCH_NEXT
              # Nothing special to do, besides the fetchrow
           when SQL_FETCH_PRIOR
              @index -= 2
           when SQL_FETCH_FIRST
              @index = -1
           when SQL_FETCH_LAST
              @index = @result.size - 2
           when SQL_FETCH_ABSOLUTE
              # Note: if you go "out of range", all fetches will give nil until you get back
              # into range, this doesn't raise an error.
              @index = offset-1
           when SQL_FETCH_RELATIVE
              # Note: if you go "out of range", all fetches will give nil until you get back
              # into range, this doesn't raise an error.
              @index += offset - 1
           else
              raise NotSupportedError
           end
           self.fetchrow
        end

        def row_count
           @fb_result.num_rows
        end

        def rows_affected
           @fb_result.num_rows
        end

        def finish
           @fb_result.clear
        end

        private # ----------------------------------------------------

        def fill_array(rowdata)
           rowdata.each_with_index { |value, index|
           case @fb_result.column_type(index)
           when FBSQL_Connect::FB_Integer,
                FBSQL_Connect::FB_PrimaryKey, 
                FBSQL_Connect::FB_SmallInteger, 
                FBSQL_Connect::FB_TinyInteger, 
                FBSQL_Connect::FB_LongInteger 
              @row[index] = @coerce.coerce(:as_int, value)
           when FBSQL_Connect::FB_Float,
                FBSQL_Connect::FB_Real, 
                FBSQL_Connect::FB_Double, 
                FBSQL_Connect::FB_Numeric, 
                FBSQL_Connect::FB_Decimal 
              @row[index] = @coerce.coerce(:as_float, value)
           when FBSQL_Connect::FB_Time,
                FBSQL_Connect::FB_TimeTZ 
              @row[index] = @coerce.coerce(:as_time, value)
           when FBSQL_Connect::FB_Timestamp,
                FBSQL_Connect::FB_TimestampTZ 
              @row[index] = @coerce.coerce(:as_timestamp, value)
           when FBSQL_Connect::FB_Date
              @row[index] = @coerce.coerce(:as_date, value)
           when FBSQL_Connect::FB_Boolean
              @row[index] = @coerce.coerce(:as_bool, value)
           when FBSQL_Connect::FB_Character, FBSQL_Connect::FB_VCharacter
              @row[index] = @coerce.coerce(:as_str, value)
           else 
              @row[index] = value
           end
           }
        end

      end # Tuples

    end # module FrontBase
  end # module DBD
end # module DBI
