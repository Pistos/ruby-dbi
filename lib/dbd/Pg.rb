#--
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
#++

begin
    require 'rubygems'
    gem 'pg'
    gem 'dbi'
rescue Exception => e
end

require 'dbi'
require 'pg'

module DBI
    module DBD
        #
        # DBD::Pg - Database Driver for the PostgreSQL database system.
        #
        # Requires DBI and the 'pg' gem or package to work.
        #
        # Only things that extend DBI's results are documented.
        #
        module Pg
            VERSION          = "0.3.3"
            DESCRIPTION      = "PostgreSQL DBI DBD"

            #
            # returns 'Pg'
            # 
            # See DBI::TypeUtil#convert for more information.
            #
            def self.driver_name
                "Pg"
            end

            #
            # This method takes a ruby Array and converts it to PostgreSQL array syntax.
            #
            def self.generate_array(obj)
                # XXX yarr, there be recursion here, and it's probably not a good idea.
                output = "{"
                obj.each do |item|
                    case item
                    when ::Array
                        output += generate_array(item)
                    else
                        generated = DBI::TypeUtil.convert(driver_name, item)
                        generated = case item
                                    when String
                                        # in strings, escapes are doubled and the quotes are different.
                                        # this gets *really* ugly and needs to be well-tested
                                        "\"#{generated.gsub(/\\/) { "\\\\" }}\""
                                    when Fixnum
                                        generated.to_s
                                    end
                        output += generated
                    end
                    output += "," # FIXME technically, delimiters are variable
                end

                output.sub(/,$/, '}')
            end

            #
            # A quote helper, this uses the new syntax in PostgreSQL 8.2 and up.
            #
            def self.quote(value)
                "E'#{ value.gsub(/\\/){ '\\\\' }.gsub(/'/){ '\\\'' } }'"
            end

            #
            # Parse a postgresql type. Returns a hash with these fields (as Symbol)
            #
            # * ftype: the full type, as passed in to this method.
            # * type: the type stripped of all attribute information.
            # * size: the LHS of the attribute information, typically the precision.
            # * decimal: the RHS of the attribute information, typically the scale.
            # * array: true if this type is actually an array of that type.
            #
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
                    :ftype   => ftype.dup,
                    :type    => type,
                    :size    => size,
                    :decimal => decimal,
                    :array   => array_of_type
                }
            end

<<<<<<< HEAD:lib/dbd/Pg.rb
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
            when 'time', 'timetz'           then :as_time
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
          #str = str.gsub(/\\[0-7]{3}/) { |match| "\\#{match}" }
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
            cols = Array.new
            0.upto(@pg_result.num_fields-1) do |x|
                cols.push({ "name" => @pg_result.fname(x) })
            end

            return cols
        end

        def fetchrow
          @index += 1
          if @index < @pg_result.num_tuples && @index >= 0
            fill_array(@index)
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

        def fill_array(rownum)
            @row = Array.new
            0.upto(@pg_result.num_fields-1) do |x|
                @row.push(@db.convert(@pg_result.getvalue(rownum, x), @pg_result.ftype(x)))
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

            # XXX 
            # String#split does not properly create a full array if the the
            # string ENDS in the split regex, unless this oddball -1 argument is supplied.
=======
>>>>>>> development:lib/dbd/Pg.rb
            #
            # See DBI::BaseDriver.
            #
            class Driver < DBI::BaseDriver
                def initialize
                    super("0.4.0")
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
require 'dbd/pg/exec'

pg = DBI::DBD::Pg

DBI::TypeUtil.register_conversion(pg.driver_name) do |obj|
    newobj = case obj
             when ::DateTime
                 obj.strftime("%m/%d/%Y %H:%M:%S.%N")
             when ::Time, ::Date
                 ::DateTime.parse(obj.to_s).strftime("%m/%d/%Y %H:%M:%S.%N")
             when ::Array
                 pg.generate_array(obj)
             when DBI::DBD::Pg::Type::ByteA
                 obj.escaped
             else
                 obj
             end
    [newobj, false]
end
