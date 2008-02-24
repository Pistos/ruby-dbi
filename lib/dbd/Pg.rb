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

module DBI
  module DBD
    module Pg
      
      VERSION          = "0.3.3"
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
        
        # by Eli Green
        POSTGRESQL_to_XOPEN = {
          "boolean"                   => [SQL_CHAR, 1, nil],
          "character"                 => [SQL_CHAR, 1, nil],
          "char"                      => [SQL_CHAR, 1, nil],
          "real"                      => [SQL_REAL, 4, 6],
          "double precision"          => [SQL_DOUBLE, 8, 15],
          "smallint"                  => [SQL_SMALLINT, 2],
          "integer"                   => [SQL_INTEGER, 4],
          "bigint"                    => [SQL_BIGINT, 8],
          "numeric"                   => [SQL_NUMERIC, nil, nil],
          "time with time zone"       => [SQL_TIME, nil, nil],
          "timestamp with time zone"  => [SQL_TIMESTAMP, nil, nil],
          "bit varying"               => [SQL_BINARY, nil, nil], #huh??
          "character varying"         => [SQL_VARCHAR, nil, nil],
          "bit"                       => [SQL_TINYINT, nil, nil],
          "text"                      => [SQL_VARCHAR, nil, nil],
          nil                         => [SQL_OTHER, nil, nil]
        }
        
        def initialize(dbname, user, auth, attr)
          hash = Utils.parse_params(dbname)

          if hash['dbname'].nil? and hash['database'].nil?
            raise DBI::InterfaceError, "must specify database"
          end

          hash['options'] ||= ''
          hash['tty'] ||= ''
          hash['port'] = hash['port'].to_i unless hash['port'].nil? 

          @connection = PGconn.new(hash['host'], hash['port'], hash['options'], hash['tty'], 
            hash['dbname'] || hash['database'], user, auth)

          @exec_method = :exec

          @attr = attr
          @attr['NonBlocking'] ||= false
          @attr.each { |k,v| self[k] = v} 

          load_type_map

          @in_transaction = false
          self['AutoCommit'] = true    # Postgres starts in unchained mode (AutoCommit=on) by default 

        rescue PGError => err
          raise DBI::OperationalError.new(err.message)
        end
        
        # DBD Protocol -----------------------------------------------

        def disconnect
          if not @attr['AutoCommit'] and @in_transaction
            _exec("ROLLBACK")   # rollback outstanding transactions
          end
          @connection.close
        end
        
        def ping
          answer = _exec("SELECT 1")
          if answer
            return answer.num_tuples == 1
          else
            return false
          end
        rescue PGError
          return false
        ensure
          answer.clear if answer
        end

        def tables
          stmt = execute("SELECT c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','v') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid)")
          res = stmt.fetch_all.collect {|row| row[0]} 
          stmt.finish
          res
        end

        ##
        # by Eli Green (cleaned up by Michael Neumann)
        #
        def columns(table)
          sql1 = %[
            SELECT a.attname, i.indisprimary, i.indisunique 
                   FROM pg_class bc, pg_class ic, pg_index i, pg_attribute a 
            WHERE bc.relkind = 'r' AND bc.relname = ? AND i.indrelid = bc.oid AND 
                  i.indexrelid = ic.oid AND ic.oid = a.attrelid
          ]

          sql2 = %[
            SELECT a.attname, a.atttypid, a.attnotnull, a.attlen, format_type(a.atttypid, a.atttypmod) 
                   FROM pg_class c, pg_attribute a, pg_type t 
            WHERE a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND c.relname = ?
          ]

          # by Michael Neumann (get default value)
          # corrected by Joseph McDonald
          sql3 = %[
            SELECT pg_attrdef.adsrc, pg_attribute.attname 
                   FROM pg_attribute, pg_attrdef, pg_class
            WHERE pg_class.relname = ? AND 
                  pg_attribute.attrelid = pg_class.oid AND
                  pg_attrdef.adrelid = pg_class.oid AND
                  pg_attrdef.adnum = pg_attribute.attnum
          ]

          dbh = DBI::DatabaseHandle.new(self)
          indices = {}
          default_values = {}

          dbh.select_all(sql3, table) do |default, name|
            default_values[name] = default
          end

          dbh.select_all(sql1, table) do |name, primary, unique|
            indices[name] = [primary, unique]
          end

          ########## 

          ret = []
          dbh.execute(sql2, table) do |sth|
            ret = sth.collect do |row|
              name, pg_type, notnullable, len, ftype = row
              #name = row[2]
              indexed = false
              primary = nil
              unique = nil
              if indices.has_key?(name)
                indexed = true
                primary, unique = indices[name]
              end

              type = ftype
              pos = ftype.index('(')
              decimal = nil
              size = nil
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
              size = len if size.nil?

              if POSTGRESQL_to_XOPEN.has_key?(type)
                sql_type = POSTGRESQL_to_XOPEN[type][0]
              else
                sql_type = POSTGRESQL_to_XOPEN[nil][0]
              end

              row = {}
              row['name']           = name
              row['sql_type']       = sql_type
              row['type_name']      = type
              row['nullable']       = ! notnullable
              row['indexed']        = indexed
              row['primary']        = primary
              row['unique']         = unique
              row['precision']      = size
              row['scale']          = decimal
              row['default']        = default_values[name]
              row
            end # collect
          end # execute

          return ret
        end

        def prepare(statement)
          Statement.new(self, statement)
        end
        
        def [](attr)
          case attr
          when 'pg_client_encoding'
            @connection.client_encoding
          else
            @attr[attr]
          end
        end

        def []=(attr, value)
          case attr
          when 'AutoCommit'
            if @attr['AutoCommit'] != value then
              if value    # turn AutoCommit ON
                if @in_transaction
                  # TODO: commit outstanding transactions?
                  _exec("COMMIT")
                  @in_transaction = false
                end
              else        # turn AutoCommit OFF
                @in_transaction = false
              end
            end
            # value is assigned below
          when 'NonBlocking'
            @exec_method = if value then :async_exec else :exec end
          when 'pg_client_encoding'
            @connection.set_client_encoding(value)
          else
            if attr =~ /^pg_/ or attr != /_/
              raise DBI::NotSupportedError, "Option '#{attr}' not supported"
            else # option for some other driver - quitly ignore
              return
            end
          end
          @attr[attr] = value
        end

        def commit
          if @in_transaction
            _exec("COMMIT")
            @in_transaction = false
          else
            # TODO: Warn?
          end
        end

        def rollback
          if @in_transaction
            _exec("ROLLBACK")
            @in_transaction = false
          else
            # TODO: Warn?
          end
        end

        # Other Public Methods ---------------------------------------


        # parse a PostgreSQL-Array output and convert into ruby array
        def convert_array( str, elemtype )

          array_nesting = 0         # nesting level of the array
          in_string = false         # currently inside a quoted string ?
          escaped = false           # if the character is escaped
          sbuffer = ''              # buffer for the current element
          result_array = Array.new  # the resulting Array
  
          str.each_byte { |char|    # parse character by character
            char = char.chr         # we need the Character, not it's Integer

            if escaped then         # if this character is escaped, just add it to the buffer
              sbuffer += char
              escaped = false
              next
            end
    
            case char               # let's see what kind of character we have
            #------------- {: beginning of an array ----#
            when '{'
              if in_string then     # ignore inside a string
                sbuffer += char
                next
              end

              if array_nesting >= 1 then  # if it's an nested array, defer for recursion
                sbuffer += char
              end
              array_nesting += 1          # inside another array
    
            #------------- ": string deliminator --------#
            when '"'
              in_string = !in_string      

            #------------- \: escape character, next is regular character #
            when '\\'     # single \, must be extra escaped in Ruby
              escaped = true

            #------------- ,: element separator ---------#
            when ','
              if in_string or array_nesting > 1 then  # don't care if inside string or
                sbuffer += char                       # nested array
              else
                if !sbuffer.is_a? Array then
                  sbuffer = convert( sbuffer, elemtype )
                end
                result_array << sbuffer               # otherwise, here ends an element
                sbuffer = ''
              end

            #------------- }: End of Array --------------#
            when '}' 
              if in_string then                # ignore if inside quoted string
                sbuffer += char
                next
              end

              array_nesting -=1                # decrease nesting level
      
              if array_nesting == 1            # must be the end of a nested array 
                sbuffer += char
                sbuffer = convert_array( sbuffer, elemtype )  # recurse, using the whole nested array
              elsif array_nesting > 1          # inside nested array, keep it for later
                sbuffer += char
              else                             # array_nesting = 0, must be the last }
                if !sbuffer.is_a? Array then
                  sbuffer = convert( sbuffer, elemtype )
                end

                result_array << sbuffer unless sbuffer.nil? # upto here was the last element
              end

            #------------- all other characters ---------#
            else
              sbuffer += char                 # simply append

            end

          } 
  
          return result_array
        end # convert_array()

        def convert(obj,typeid)
          return nil if obj.nil?
          
          if @elem_map.include?( typeid ) then
            convert_array( obj, @elem_map[ typeid ] )
          else
            converter = @type_map[typeid] || :as_str
            #raise DBI::InterfaceError, "Unsupported Type (typeid=#{typeid})" if converter.nil?
            @coerce.coerce(converter, obj)
          end

        end

        def in_transaction?
          @in_transaction
        end

        def start_transaction
          _exec("BEGIN")
          @in_transaction = true
        end

        def _exec(sql)
          @connection.send(@exec_method, sql)
        end

     if PGconn.respond_to?(:quote)

        def quote(value)
          if value.kind_of? Array then # work around broken PGconn.quote for Arrays
            "'#{ quote_array_elements( value ).gsub(/\\/){ '\\\\' }.gsub(/'/){ '\\\'' } }'"
          else
            PGconn.quote(value) {|value|
              case value
              when DBI::Date, DBI::Time, DBI::Timestamp, ::Date, ::Time
                "'#{value.to_s}'"
              else
                value.to_s
              end
            }
          end
        end

      else

        def quote(value)
          case value
          when String
            "'#{ value.gsub(/\\/){ '\\\\' }.gsub(/'/){ '\\\'' } }'"
          when Array
            "'#{ quote_array_elements( value ).gsub(/\\/){ '\\\\' }.gsub(/'/){ '\\\'' } }'"
          else
            super
          end
        end

      end

        
        private # ----------------------------------------------------

        # special quoting if value is element of an array 
        def quote_array_elements( value )
          case value
          when Array
            '{'+ value.collect{|v| quote_array_elements(v) }.join(',') + '}'
          when String
            '"' + value.gsub(/\\/){ '\\\\' }.gsub(/"/){ '\\"' } + '"'
          else
            quote( value ).sub(/^'/,'').sub(/'$/,'') 
          end
        end 
        
        def load_type_map
          @type_map = Hash.new
          @elem_map = Hash.new
          @coerce = PgCoerce.new

          res = _exec("SELECT oid, typname, typelem FROM pg_type WHERE typtype = 'b';")

          res.each do |row|
            @type_map[row["oid"].to_i] = 
            case row["typname"]
            when 'bool'                      then :as_bool
            when 'int8', 'int4', 'int2'    then :as_int
            when 'varchar'                   then :as_str
            when 'float4','float8'          then :as_float
            when 'timestamp', 'timestamptz' then :as_timestamp
            when 'date'                      then :as_date
            when 'bytea'                     then :as_bytea
            else
              if row["typname"] =~ /^_/ and row["typelem"].to_i > 0 then
                @elem_map[row["typname"].to_i] = row["typelem"].to_i
                :as_str
              else
                :as_str
              end
            end
          end 
          # additional conversions
          @type_map[705]  ||= :as_str       # select 'hallo'
          @type_map[1114] ||= :as_timestamp # TIMESTAMP WITHOUT TIME ZONE
        end


        # Driver-specific functions ------------------------------------------------

        public

        def __blob_import(file)
          start_transaction unless @in_transaction
          @connection.lo_import(file)
          #if @attr['AutoCommit']
          #  _exec("COMMIT")
          #  @in_transaction = false
          #end
        rescue PGError => err
          raise DBI::DatabaseError.new(err.message) 
        end

        def __blob_export(oid, file)
          start_transaction unless @in_transaction
          @connection.lo_export(oid.to_i, file)
          #if @attr['AutoCommit']
          #  _exec("COMMIT")
          #  @in_transaction = false
          #end
        rescue PGError => err
          raise DBI::DatabaseError.new(err.message) 
        end

        def __blob_create(mode=PGconn::INV_READ)
          start_transaction unless @in_transaction
          @connection.lo_creat(mode)
          #if @attr['AutoCommit']
          #  _exec("COMMIT")
          #  @in_transaction = false
          #end
        rescue PGError => err
          raise DBI::DatabaseError.new(err.message) 
        end

        def __blob_open(oid, mode=PGconn::INV_READ)
          start_transaction unless @in_transaction
          @connection.lo_open(oid.to_i, mode)
          #if @attr['AutoCommit']
          #  _exec("COMMIT")
          #  @in_transaction = false
          #end
        rescue PGError => err
          raise DBI::DatabaseError.new(err.message) 
        end

        def __blob_unlink(oid)
          start_transaction unless @in_transaction
          @connection.lo_unlink(oid.to_i)
          #if @attr['AutoCommit']
          #  _exec("COMMIT")
          #  @in_transaction = false
          #end
        rescue PGError => err
          raise DBI::DatabaseError.new(err.message) 
        end

        def __blob_read(oid, length)
            blob = @connection.lo_open(oid.to_i, PGconn::INV_READ)

            if length.nil?
                data = @connection.lo_read(blob)
            else
                data = @connection.lo_read(blob, length)
            end

            # FIXME it doesn't like to close here either.
            # @connection.lo_close(blob)
            data
        rescue PGError => err
            raise DBI::DatabaseError.new(err.message) 
        end
        
        def __blob_write(oid, value)
            start_transaction unless @in_transaction
            blob = @connection.lo_open(oid.to_i, PGconn::INV_WRITE)
            res = @connection.lo_write(blob, value)
            # FIXME not sure why PG doesn't like to close here -- seems to be
            # working but we should make sure it's not eating file descriptors
            # up before release.
            # @connection.lo_close(blob)
            return res
        rescue PGError => err
            raise DBI::DatabaseError.new(err.message)
        end

	def __set_notice_processor(proc)
	  @connection.set_notice_processor proc
	rescue PGError => err
	  raise DBI::DatabaseError.new(err.message) 
	end

     if PGconn.respond_to?(:escape_bytea)

        def __encode_bytea(str)
          # FIXME there's a bug in the upstream 'pg' driver that does not
          # properly encode bytea, improperly handling "\123" treating it as
          # an octet.
          # 
          # Fix this for now, but beware that we'll have to unfix this as
          # soon as they fix their end.
          str = str.gsub(/\\([0-7]{3})/, "\\\\\1")
          @connection.escape_bytea(str)
        end

      else

        ##
        # encodes a string as bytea value.
        #
        # for encoding rules see:
        #   http://www.postgresql.org/idocs/index.php?datatype-binary.html
        #
        def __encode_bytea(str)
          # TODO: use quote function of Pg driver
          a = str.split(/\\/, -1).collect! {|s|
            s.gsub!(/'/,    "\\\\047")  # '  => \\047 
            s.gsub!(/\000/, "\\\\000")  # \0 => \\000  
            s
          }
          a.join("\\\\")                # \  => \\
        end

      end

      end # Database

      ################################################################
      class Statement < DBI::BaseStatement
        
        def initialize(db, sql)
          @db  = db
          @prep_sql = DBI::SQL::PreparedStatement.new(@db, sql)
          @result = nil
          @bindvars = []
        end
        
        def bind_param(index, value, options)
          @bindvars[index-1] = value
        end

        def execute
          # replace DBI::Binary object by oid returned by lo_import 
          @bindvars.collect! do |var|
            if var.is_a? DBI::Binary then
              oid = @db.__blob_create(PGconn::INV_WRITE)
              @db.__blob_write(oid, var.to_s)
              oid 
            else
              var
            end
          end

          boundsql = @prep_sql.bind(@bindvars)

          if not @db['AutoCommit'] then
#          if not SQL.query?(boundsql) and not @db['AutoCommit'] then
            @db.start_transaction unless @db.in_transaction?
          end
          pg_result = @db._exec(boundsql)
          @result = Tuples.new(@db, pg_result)

        rescue PGError, RuntimeError => err
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
          when 'pg_row_count'
            if @result
              @result.row_count
            else
              nil
            end
          else
            @attr[attr]
          end
        end


        private # ----------------------------------------------------

      end # Statement
      
      ################################################################
      class Tuples

        def initialize(db,pg_result)
          @db = db
          @pg_result = pg_result
          @index = -1
          @row = Array.new
        end

        def column_info
          @pg_result.fields.collect do |str| {'name'=>str} end
        end

        def fetchrow
          @index += 1
          if @index < @pg_result.num_tuples && @index >= 0
            fill_array(@pg_result[@index])
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
            @index = @pg_result.num_tuples - 2
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
          @pg_result.num_tuples
        end

        def rows_affected
          @pg_result.cmdtuples
        end

        def finish
          @pg_result.clear
        end

        private # ----------------------------------------------------

        def fill_array(rowdata)
            rowdata.each do |key, value|
                @row[@pg_result.fnumber(key)] = @db.convert(value,@pg_result.ftype(@pg_result.fnumber(key)))
            end
        end

      end # Tuples

      ################################################################
      class PgCoerce < DBI::SQL::BasicQuote::Coerce
        #
        # for decoding rules see:
        #   http://www.postgresql.org/idocs/index.php?datatype-binary.html
        #
        def as_bytea(str)
            # FIXME there's a bug in the upstream 'pg' driver that does not
            # properly decode bytea, leaving in an extra slash for each decoded
            # character.
            #
            # Fix this for now, but beware that we'll have to unfix this as
            # soon as they fix their end.
            ret = PGconn.unescape_bytea(str)
            ret.gsub!(/\\\\/, "\\")
            ret.gsub!(/\\[0-7]{3}/) { |x| x[1..3].to_i(8).chr }
            ret.gsub!(/''/, "'")
            return ret
        end

        def as_timestamp(str)
	  return super unless m = /\.\d+(?=(?:[-+]\d+)?$)/.match(str)
	  (t = super $` + $').fraction = m.to_s.to_f
	  t
        end
      end

    end # module Pg
  end # module DBD
end # module DBI
