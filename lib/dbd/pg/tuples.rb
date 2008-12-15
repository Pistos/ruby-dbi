#
# Tuples is a class to represent result sets.
#
# Many of these methods are extremely similar to the methods that deal with
# result sets in DBI::BaseStatement and are wrapped by the StatementHandle.
# Unless you plan on working on this driver, these methods should never be
# called directly.
#
class DBI::DBD::Pg::Tuples

    def initialize(db, pg_result)
        @db = db
        @pg_result = pg_result
        @index = -1
        @row = []
    end

    # See DBI::BaseStatement#column_info. Additional attributes:
    #
    # * array_of_type: True if this is actually an array of this type. In this
    #   case, +dbi_type+ will be the type authority for conversion.
    #
    def column_info
        a = []
        0.upto(@pg_result.num_fields-1) do |i|
            str = @pg_result.fname(i)

            typeinfo = nil

            begin
                typmod = @pg_result.fmod(i)
            rescue
            end

            if typmod and typ = @pg_result.ftype(i)
                res = @db._exec("select format_type(#{typ}, #{typmod})")
                typeinfo = DBI::DBD::Pg.parse_type(res[0].values[0])
            end

            map = @db.type_map[@pg_result.ftype(i)] || { }
            h = { "name" => str }.merge(map)

            if typeinfo
                h["precision"]     = typeinfo[:size]
                h["scale"]         = typeinfo[:decimal]
                h["type"]          = typeinfo[:type]
                h["array_of_type"] = typeinfo[:array]

                if typeinfo[:array]
                    h['dbi_type'] = 
                        DBI::DBD::Pg::Type::Array.new(
                            DBI::TypeUtil.type_name_to_module(typeinfo[:type])
                    )
                end
            end

            a.push h
        end

        return a
    end

    def fetchrow
        @index += 1
        if @index < @pg_result.num_tuples && @index >= 0
            @row = Array.new
            0.upto(@pg_result.num_fields-1) do |x|
                @row.push(@pg_result.getvalue(@index, x))
            end
            @row
        else
            nil
        end
    end

    #
    # Just don't use this method. It'll be fixed soon.
    #
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

    #
    # The number of rows returned.
    #
    def row_count
        @pg_result.num_tuples
    end

    #
    # The row processed count. This is analogue to DBI::StatementHandle#rows.
    #
    def rows_affected
        @pg_result.cmdtuples
    end

    def finish
        @pg_result.clear
    end
end # Tuples
