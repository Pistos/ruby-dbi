#
# See DBI::BaseStatement.
#
class DBI::DBD::SQLite3::Statement < DBI::BaseStatement
    def initialize(sql, db)
        sql.gsub!(/\\\\/) { '\\' } # sqlite underneath does this for us automatically, and it's causing trouble with the rest of the system.
        @sql = sql
        @db = db
        @stmt = db.prepare(sql)
        @result = nil
    rescue ::SQLite3::Exception, RuntimeError => err
        raise DBI::ProgrammingError.new(err.message)
    end

    def bind_param(param, value, attribs=nil)
        raise DBI::InterfaceError, "Bound parameter must be an integer" unless param.kind_of? Fixnum 
        @stmt.bind_param(param, value)
    end

    def execute()
        @result = @stmt.execute
        @rows = DBI::SQL.query?(@sql) ? 0 : @db.changes
    end

    def finish()
        @stmt.close rescue nil
        @result = nil
    end

    def fetch()
        ret = @result.next
        return ret unless ret
        [ret].flatten
    end

    def column_info()
        @stmt.columns.zip(@stmt.types).map{|name, type_name|
            m = DBI::DBD::SQLite3.parse_type(type_name)
            h = { 
              'name' => name,
              'type_name' => m[1],
              'sql_type' => 
                    begin
                          DBI.const_get('SQL_'+m[1].upcase)
                    rescue NameError
                        DBI::SQL_OTHER
                    end,
            }
            h['precision'] = m[3].to_i if m[3]
            h['scale']     = m[5].to_i if m[5]
            h
        }
    end

    def rows()
        @rows
    end

    def bind_params(*bindvars)
        @stmt.bind_params(bindvars)
    end

    def cancel()
        @result = nil
        @index = 0
    end
end
