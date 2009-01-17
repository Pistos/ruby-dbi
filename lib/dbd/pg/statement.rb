#
# See DBI::BaseStatement, and DBI::DBD::Pg::Tuples.
#
#--
# Peculiar Statement responsibilities:
#  - Translate dbi params (?, ?, ...) to Pg params ($1, $2, ...)
#  - Translate DBI::Binary objects to Pg large objects (lo_*)

class DBI::DBD::Pg::Statement < DBI::BaseStatement

    PG_STMT_NAME_PREFIX = 'ruby-dbi:Pg:'

    def initialize(db, sql)
        super(db)
        @db  = db
        @sql = sql
        @stmt_name = PG_STMT_NAME_PREFIX + self.object_id.to_s
        @result = nil
        @bindvars = []
        @prepared = false
    rescue PGError => err
        raise DBI::ProgrammingError.new(err.message)
    end

    def bind_param(index, value, options)
        @bindvars[index-1] = value
    end

    #
    # See DBI::BaseDatabase#execute.
    #
    # This method will make use of PostgreSQL's native BLOB support if
    # DBI::Binary objects are passed in.
    #
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
        
        internal_prepare

        if not @db['AutoCommit'] then
            #          if not SQL.query?(boundsql) and not @db['AutoCommit'] then
            @db.start_transaction unless @db.in_transaction?
        end

        if @db["pg_native_binding"]
            pg_result = @db._exec_prepared(@stmt_name, *@bindvars)
        else
            pg_result = @db._exec_prepared(@stmt_name)
        end

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
        internal_finish
        @result = nil
        @db = nil
    end

    #
    # See DBI::DBD::Pg::Tuples#column_info.
    #
    def column_info
        @result.column_info
    end

    def rows
        if @result
            @result.rows_affected
        else
            nil
        end
    end

    #
    # Attributes:
    # 
    # If +pg_row_count+ is requested and the statement has already executed,
    # postgres will return what it believes is the row count.
    #
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

    private 

    #
    # A native binding helper.
    #
    class DummyQuoter
        # dummy to substitute ?-style parameter markers by :1 :2 etc.
        def quote(str)
            str
        end
    end

    # finish the statement at a lower level
    def internal_finish
        @result.finish if @result
        @db._exec("DEALLOCATE \"#{@stmt_name}\"") if @prepared rescue nil
    end

    # prepare the statement at a lower level.
    def internal_prepare
        if @db["pg_native_binding"]
            unless @prepared
                @stmt = @db._prepare(@stmt_name, translate_param_markers(@sql))
            end
        else
            internal_finish
            @stmt = @db._prepare(@stmt_name, DBI::SQL::PreparedStatement.new(DBI::DBD::Pg, @sql).bind(@bindvars))
        end
        @prepared = true
    end

    # Prepare the given SQL statement, returning its PostgreSQL string
    # handle.  ?-style parameters are translated to $1, $2, etc.
    #--
    # TESTME  do ?::TYPE qualifers work?
    # FIXME:  DBI ought to supply a generic param converter, e.g.:
    #         sql = DBI::Utils::convert_placeholders(sql) do |i|
    #                 '$' + i.to_s
    #               end
    def translate_param_markers(sql)
        translator = DBI::SQL::PreparedStatement.new(DummyQuoter.new, sql)
        if translator.unbound.size > 0
            arr = (1..(translator.unbound.size)).collect{|i| "$#{i}"}
            sql = translator.bind( arr )
        end
        sql
    end
end # Statement
