class DBI::DBD::SQLite::Statement < DBI::BaseStatement
    DBI_TYPE_MAP = [
        [ /^INT(EGER)?$/i,          DBI::SQL_INTEGER ],
        [ /^(OID|ROWID|_ROWID_)$/i, DBI::SQL_OTHER   ],
        [ /^FLOAT$/i,               DBI::SQL_FLOAT   ],
        [ /^REAL$/i,                DBI::SQL_REAL    ],
        [ /^DOUBLE$/i,              DBI::SQL_DOUBLE  ],
        [ /^DECIMAL/i,              DBI::SQL_DECIMAL ],
        [ /^(BOOL|BOOLEAN)$/i,      DBI::SQL_BOOLEAN ], 
        [ /^TIME$/i,                DBI::SQL_TIME    ],
        [ /^DATE$/i,                DBI::SQL_DATE    ],
        [ /^TIMESTAMP$/i,           DBI::SQL_TIMESTAMP ], 
        [ /^(VARCHAR|TEXT)/i,       DBI::SQL_VARCHAR ],
        [ /^CHAR$/i,                DBI::SQL_CHAR    ],
    ]

    def initialize(stmt, dbh)
        @dbh       = dbh
        @statement = DBI::SQL::PreparedStatement.new(@dbh, stmt)
        @attr      = { }
        @params    = [ ]
        @rows      = [ ]
        @result_set = nil
        @dbh.open_handles += 1
    end

    def bind_param(param, value, attributes=nil)
        unless param.kind_of? Fixnum
            raise DBI::InterfaceError, "Only numeric parameters are supported"
        end

        @params[param-1] = value

        # FIXME what to do with attributes? are they important in SQLite?
    end

    def execute
        sql = @statement.bind(@params)
        DBI::DBD::SQLite.check_sql(sql)

        begin
            unless @dbh.db.transaction_active?
                @dbh.db.transaction 
            end
            @result_set = @dbh.db.query(sql)
            @dbh.commit if @dbh["AutoCommit"]
        rescue Exception => e
            raise DBI::DatabaseError, e.message
        end
    end

    alias :finish :cancel

    def finish
        # nil out the result set
        @result_set.close if @result_set
        @result_set = nil
        @rows = nil
        @dbh.open_handles -= 1
    end

    def fetch
        return nil if @result_set.eof?

        row = @result_set.next
        return nil unless row

        return row
    end

    def column_info
        columns = [ ]

        # FIXME this shit should *really* be abstracted into DBI
        # FIXME this still doesn't handle nullable/unique/default stuff.
        @result_set.columns.each_with_index do |name, i|
            columns[i] = { } unless columns[i]
            columns[i]["name"] = name
            type_name = @result_set.types[i]

            if type_name
                m = DBI::DBD::SQLite.parse_type(type_name)

                columns[i]["type_name"] = m[1]
                columns[i]["precision"] = m[3].to_i if m[3]
                columns[i]["scale"]     = m[5].to_i if m[5]
                DBI_TYPE_MAP.each do |map|
                    if columns[i]["type_name"] =~ map[0]
                        columns[i]["sql_type"] = map[1]
                        break
                    end
                end
            end
        end

        return columns
    end

    def rows
        return @dbh.db.changes
    end
end
