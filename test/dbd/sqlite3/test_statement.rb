class TestStatement < DBDConfig.testbase(:sqlite3)
    def test_constructor
        sth = DBI::DBD::SQLite3::Statement.new("select * from names", @dbh.instance_variable_get("@handle").instance_variable_get("@db"))

        assert_kind_of DBI::DBD::SQLite3::Statement, sth
        assert sth.instance_variable_get("@db")
        assert_kind_of ::SQLite3::Database, sth.instance_variable_get("@db")
        assert_equal(@dbh.instance_variable_get("@handle").instance_variable_get("@db"), sth.instance_variable_get("@db"))
        assert_kind_of ::SQLite3::Statement, sth.instance_variable_get("@stmt")
        assert_nil(@sth.instance_variable_get("@result"))

        sth.finish

        sth = @dbh.prepare("select * from names")

        assert_kind_of DBI::StatementHandle, sth
        sth.finish
    end

    def test_bind_param
        sth = DBI::DBD::SQLite3::Statement.new("select * from names", @dbh.instance_variable_get("@handle").instance_variable_get("@db"))

        assert_raises(DBI::InterfaceError) do
            sth.bind_param(:foo, "monkeys")
        end

        sth.finish
    end
    
    def test_column_info
        @sth = nil
        
        assert_nothing_raised do 
            @sth = @dbh.prepare("select * from names")
            @sth.execute
        end

        assert_kind_of Array, @sth.column_info 
        assert_kind_of DBI::ColumnInfo, @sth.column_info[0]
        assert_kind_of DBI::ColumnInfo, @sth.column_info[1]
        assert_equal [ 
            { 
                :name  => "name",
                :sql_type  => 12,
                :precision  => 255,
                :type_name  => "varchar"
            }, 
            { 
                :name  => "age",
                :sql_type  => 4,
                :type_name  => "integer"
            } 
        ], @sth.column_info

        @sth.finish
    end
    
    def test_specific_types
        assert_nothing_raised do
            @sth = @dbh.prepare("insert into db_specific_types_test (dbl) values (?)")
            @sth.execute(11111111.111111)
            @sth.execute(22)
            @sth.finish
        end

        assert_nothing_raised do
            @sth = @dbh.prepare("select * from db_specific_types_test")
            @sth.execute
            assert_equal([11111111.111111], @sth.fetch)
            assert_equal([22], @sth.fetch)
            @sth.finish

            assert_equal([[11111111.111111], [22]], @dbh.select_all("select * from db_specific_types_test"))
        end
    end
end
