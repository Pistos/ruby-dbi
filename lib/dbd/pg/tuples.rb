class DBI::DBD::Pg::Tuples

    def initialize(db,pg_result)
        @db = db
        @pg_result = pg_result
        @index = -1
        @row = Array.new
    end

    def column_info
        a = []
        @pg_result.fields.each_with_index do |str, i| 
            h = { "name" => str }.merge(@db.type_map[@pg_result.ftype(i)])
            a.push h
        end

        return a
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
            @row[@pg_result.fnumber(key)] = value
        end
    end
end # Tuples