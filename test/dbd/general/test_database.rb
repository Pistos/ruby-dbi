@class = Class.new(DBDConfig.testbase(DBDConfig.current_dbtype)) do
    def test_ping
        assert @dbh.ping
        # XXX if it isn't obvious, this should be tested better. Not sure what
        # good behavior is yet.
    end

    def test_columns
        assert_nothing_raised do
            cols = @dbh.columns("names")

            assert(cols)
            assert_kind_of(Array, cols)
            assert_equal(2, cols.length)

            # the first column should always be "name" and have the following
            # properties:
            assert_equal("name", cols[0]["name"])
            assert(cols[0]["nullable"])
            assert_equal(
                DBI::Type::Varchar, 
                DBI::TypeUtil.type_name_to_module(cols[0]["type_name"])
            )

            # the second column should always be "age" and have the following
            # properties:
            assert_equal("age", cols[1]["name"])
            assert(cols[1]["nullable"])
            assert_equal(
                DBI::Type::Integer, 
                DBI::TypeUtil.type_name_to_module(cols[1]["type_name"])
            )

            # finally, we ensure that every column in the array is a ColumnInfo
            # object
            cols.each { |col| assert_kind_of(DBI::ColumnInfo, col) }
        end
    end

    def test_prepare
        @sth = @dbh.prepare('select * from names')

        assert @sth
        assert_kind_of DBI::StatementHandle, @sth

        @sth.finish
    end

    def test_do
        assert_equal 1, @dbh.do("insert into names (name, age) values (?, ?)", "Billy", 21)
        @sth = @dbh.prepare("select * from names where name = ?")
        @sth.execute("Billy")
        assert_equal ["Billy", 21], @sth.fetch
        @sth.finish
    end

    def test_tables
        tables = @dbh.tables.sort

        # since this is a general test, let's prune the system tables
        # FIXME not so sure if this should be a general test anymore.
        case dbtype 
        when "postgresql"
            tables.reject! { |x| x =~ /^pg_/ }
            assert_equal %w(array_test bit_test blob_test boolean_test bytea_test field_types_test names precision_test time_test timestamp_test view_names), tables
        else
            assert_equal %w(bit_test blob_test boolean_test field_types_test names precision_test time_test timestamp_test view_names), tables
        end
    end

    def test_attrs
        # test defaults
        assert @dbh["AutoCommit"] # should be true

        # test setting
        assert !(@dbh["AutoCommit"] = false)
        assert !@dbh["AutoCommit"]

        # test committing an outstanding transaction
        
        @sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
        @sth.execute("Billy", 22)
        @sth.finish

        assert @dbh["AutoCommit"] = true # should commit at this point
        
        @sth = @dbh.prepare("select * from names where name = ?")
        @sth.execute("Billy")
        assert_equal [ "Billy", 22 ], @sth.fetch
        @sth.finish
    end
end
