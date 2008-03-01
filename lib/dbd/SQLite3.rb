#
# DBD::SQLite3
# 
# copyright (c) 2005 Jun Mukai <mukai@jmuk.org>
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
rescue LoadError
end

require 'sqlite3'
require 'sqlite3/version'

module DBI
  module DBD
    module SQLite3

      VERSION = ::SQLite3::Version::STRING
      USED_DBD_VERSION='0.2'

      class Driver < DBI::BaseDriver
        def initialize
          @dbs = []
        end

        def connect(dbname, user, auth, attr)
          db = Database.new(dbname, attr)
          @dbs.push(db)
          db
        end

        def disconnect_all()
          @dbs.each{|db| db.disconnect()}
        end
      end

      class Database < DBI::BaseDatabase
        def initialize(dbname, attr)
          @db = ::SQLite3::Database.new(dbname)

          @db.type_translation = true
          @db.translator.add_translator(nil) do |type, value|
            # autodetect numbers in typeless columns
            case value
            when /\A-?[0-9]+\z/
              value.to_i
            when /\A-?[0-9]+?\.[0-9]+\z/
              value.to_f
            else
              value
            end 
          end

          @attr = {'AutoCommit' => true}
          if attr then
            attr.each_pair do |key, value|
              begin
                self[key] = value
              rescue NotSupportedError
              end
            end
          end
          __generate_attr__
        end

        def disconnect()
          @db.rollback if @db.transaction_active?
          @db.close
        end

        def prepare(statement)
          Statement.new(statement, @db)
        end

        def ping()
          not @db.closed?
        end

        def commit()
          if @db.transaction_active?
            @db.commit
            @db.transaction
          else
            raise DBI::ProgrammingError.new("No active transaction.")
          end
        end

        def rollback()
          if @db.transaction_active?
            @db.rollback
            @db.transaction
          else
            raise DBI::ProgrammingError.new("No active transaction.")
          end
        end

        def tables()
          ret = []
          result = @db.execute(<<'EOS')
SELECT name FROM sqlite_master WHERE type IN ('table', 'view') 
UNION ALL 
SELECT name FROM sqlite_temp_master WHERE type in ('table', 'view') ORDER BY 1
EOS
          result.each{|row| ret.push(row[0])}
          ret
        end

        def columns(table)
          @db.type_translation = false
          ret =
            @db.table_info(table).map do |cid, name, type, nullable, default|
            { 'name' => name,
              'type_name' => type,
              'type' => begin
                          DBI.const_get('SQL_'+type.upcase)
                        rescue NameError
                          DBI::SQL_OTHER
                        end,
              'nullable' => (nullable == '0'),
              'default' => if @attr['type_translation'] && (not default) then
                             @db.translator.translate(type, default)
                           else
                             default
                           end
            }
          end
          @db.type_translation = @attr['type_translation']
          ret
        end

        def quote(value)
          ::SQLite3::Database.quote(value.to_s)
        end

        def __generate_attr__()
          tt = @db.type_translation
          @db.type_translation = false
          [ 'auto_vacuum', 'cache_size', 'default_cache_size',
            'default_synchronous', 'default_temp_store', 'full_column_names',
            'synchronous', 'temp_store', 'type_translation' ].each do |key|
            unless @attr.has_key?(key) then
              @attr[key] = @db.__send__(key)
            end
          end
          @db.type_translation = tt
        end

        def []=(attr, value)
          case attr
          when 'AutoCommit'
            if value
              @db.commit if @db.transaction_active?
            else
              @db.transaction unless @db.transaction_active?
            end
          when 'auto_vacuum', 'cache_size', 'count_changes',
              'default_cache_size', 'encoding', 'full_column_names',
              'page_size', 'short_column_names', 'synchronous',
              'temp_store', 'temp_store_directory'
            @db.__send__((attr+'='), value)
            @attr[attr] = @db.__send__(attr)
          when 'busy_timeout'
            @db.busy_timeout(value)
            @attr[attr] = value
          when 'busy_handler'
            @db.busy_timeout(&value)
            @attr[attr] = value
          when 'type_translation'
            @db.type_translation = value
            @attr[attr] = value
          else
            raise NotSupportedError
          end
        end
      end

      class Statement < DBI::BaseStatement
        def initialize(sql, db)
          @sql = sql
          @db = db
          @stmt = db.prepare(sql)
          @result = nil
          @rpc = nil
        rescue ::SQLite3::Exception, RuntimeError => err
          raise DBI::ProgrammingError.new(err.message)
        end

        def bind_param(param, value, attribs)
          @stmt.bind_param(param, value)
        end

        def execute()
          @result = @stmt.execute
          @rpc = 0
        end

        def finish()
          @stmt.close unless @stmt.closed?
          @result = nil
          @rpc = nil
        end

        def fetch()
          @rpc += 1
          @result.next
        end

        def column_info()
          @stmt.columns.zip(@stmt.types).map{|name, type_name|
            { 'name' => name,
              'type_name' => type_name,
              'sql_type' => begin
                              DBI.const_get('SQL_'+type_name.upcase)
                            rescue NameError
                              DBI::SQL_OTHER
                            end,
            }
          }
        end

        def rows()
          @rpc
        end

        def bind_params(*bindvars)
          @stmt.bind_params(bindvars)
        end

        def cancel()
          @result = nil
          @index = 0
        end

        def fetch_many(cnt)
          ret = nil
          if @result && (not @result.eof?) then
            ret = []
            cnt.times{ ret.push(@result.next()) }
            ret.compact!
          end
          ret
        end

        def fetch_all()
          ret = nil
          if @result then
            ret = []
            @result.each{|row| ret.push(row)}
          end
          ret
        end
      end
    end
  end
end
