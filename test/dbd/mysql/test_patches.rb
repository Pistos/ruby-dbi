class TestMysqlPatches < DBDConfig.testbase(:mysql)
    def test_exception_on_aggregate
        assert_nothing_raised do
            sth = @dbh.prepare("select sum(age) from names")
            sth.execute
            row = sth.fetch 
            # FIXME: this is BROKEN - should not be a string, should be fixnum
            assert_equal(70.0, row[0])
            sth.finish
        end
    end

    # FIXME when the spec is more solid, this should be in the general tests
    def test_columns
        assert_nothing_raised do
            assert_equal [
                {
                    "name"=>"foo",
                    "default"=>"1",
                    "primary"=>true,
                    "scale"=>nil,
                    "sql_type"=>4,
                    "nullable"=>false,
                    "indexed"=>true,
                    "precision"=>11,
                    "type_name"=>"int",
                    "unique"=>true
                }
            ], @dbh.columns("field_types_test")
        end

        assert_nothing_raised do
            sth = @dbh.prepare("insert into field_types_test (foo) values (?)")
            sth.execute(2)
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from field_types_test")
            sth.execute
            row = sth.fetch
            columns = sth.column_info
            sth.finish

            assert_equal [2], row
            assert_equal [
                {
                      "name"=>"foo",
                      "mysql_type_name"=>"INT",
                      "mysql_max_length"=>1,
                      "primary"=>true,
                      "scale"=>0,
                      "mysql_flags"=>49155,
                      "sql_type"=>4,
                      "nullable"=>false,
                      "mysql_type"=>3,
                      "indexed"=>true,
                      "mysql_length"=>11,
                      "precision"=>11,
                      "type_name"=>"INTEGER",
                      "unique"=>true
                }
            ], columns
        end
    end
end
