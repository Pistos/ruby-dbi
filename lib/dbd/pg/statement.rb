class DBI::DBD::Pg::Statement < DBI::BaseStatement

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
        @result = DBI::DBD::Pg::Tuples.new(@db, pg_result)

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
