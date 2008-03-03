class TestSQLiteStatement < SQLiteUnitBase
    def test_constructor
        sth = DBI::DBD::SQLite::Statement.new("select * from foo", @dbh.instance_variable_get("@handle"))

        assert_kind_of DBI::DBD::SQLite::Statement, sth
        assert sth.instance_variable_get("@dbh")
        assert_kind_of DBI::DBD::SQLite::Database, sth.instance_variable_get("@dbh")
        assert_equal(@dbh.instance_variable_get("@handle"), sth.instance_variable_get("@dbh"))
        assert_kind_of DBI::SQL::PreparedStatement, sth.instance_variable_get("@statement")
        assert_equal({ }, sth.instance_variable_get("@attr"))
        assert_equal([ ], sth.instance_variable_get("@params"))
        assert_nil(sth.instance_variable_get("@result_set"))
        assert_equal([ ], sth.instance_variable_get("@rows"))

        sth = @dbh.prepare("select * from foo")

        assert_kind_of DBI::StatementHandle, sth
    end

    def test_bind_param
        sth = DBI::DBD::SQLite::Statement.new("select * from foo", @dbh.instance_variable_get("@handle"))

        assert_raise(DBI::InterfaceError) do
            sth.bind_param(:foo, "monkeys")
        end

        for test_sth in [sth, @dbh.prepare("select * from foo")] do
            test_sth.bind_param(1, "monkeys", nil)

            params = test_sth.instance_variable_get("@params") || test_sth.instance_variable_get("@handle").instance_variable_get("@params")

            assert_equal "monkeys", params[0]

            # set a bunch of stuff.
            %w(I like monkeys).each_with_index { |x, i| test_sth.bind_param(i+1, x) }

            params = test_sth.instance_variable_get("@params") || test_sth.instance_variable_get("@handle").instance_variable_get("@params")
            
            assert_equal %w(I like monkeys), params

            # FIXME what to do with attributes? are they important in SQLite?
        end
    end

    def test_rows
        sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Bill", 22);
        end

        assert 1, sth.rows

        sth.finish
        sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("delete from names where name = ?")
            sth.execute("Bill");
        end

        assert 1, sth.rows

        sth.finish
    end

    def test_column_info
        sth = nil
        
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names")
            sth.execute
        end

        assert_kind_of Array, sth.column_info 
        assert_kind_of ColumnInfo, sth.column_info[0]
        assert_kind_of ColumnInfo, sth.column_info[1]
        assert_equal [ 
            { 
                "name" => "name",
                "sql_type" => 12,
                "precision" => 255,
                "type_name" => "varchar"
            }, 
            { 
                "name" => "age",
                "sql_type" => 4,
                "type_name" => "integer"
            } 
        ], sth.column_info

        sth.finish
    end

    def test_execute
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names")
            sth.execute
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from names where name = ?")
            sth.execute("Bob")
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Bill", 22);
            sth.finish
        end
    end

    def test_execute_with_transactions
        @dbh["AutoCommit"] = false 
        config = DBDConfig.get_config['sqlite']

        # rollback 1 (the right way)
        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end
        assert_equal ["Billy", 23 ], sth2.fetch
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.rollback }

        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_nil sth.fetch
        sth.finish
       
        # rollback 2 (without closing statements first)

        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end

        assert_equal ["Billy", 23], sth2.fetch
        assert_raise(DBI::Warning) { @dbh.rollback }
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.rollback }
        
        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_nil sth.fetch
        sth.finish

        # commit

        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end
        assert_equal ["Billy", 23 ], sth2.fetch
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.commit }

        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_equal ["Billy", 23 ], sth.fetch
        sth.finish
    end

    def test_fetch
        sth = nil
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names order by age")
            sth.execute
        end

        # this tests that we're getting the rows in the right order,
        # and that the types are being converted. 
        assert_equal ["Joe", 19], sth.fetch
        assert_equal ["Bob", 21], sth.fetch
        assert_equal ["Jim", 30], sth.fetch
        assert_nil sth.fetch

        sth.finish
    end
end
