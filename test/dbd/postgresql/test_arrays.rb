class TestPostgresArray < DBDConfig.testbase(:postgresql)
    def test_array_type
        assert_nothing_raised do
            cols = @dbh.columns("array_test")
            assert(cols)
            assert_equal(
                [
                    {
                        "name"=>"foo", 
                        "default"=>nil, 
                        "primary"=>nil, 
                        "scale"=>nil, 
                        "sql_type"=>DBI::SQL_OTHER, 
                        "nullable"=>true, 
                        "indexed"=>false, 
                        "precision"=>-1, 
                        "type_name"=>"integer[]", 
                        "unique"=>nil
                    }, 
                    {
                        "name"=>"bar", 
                        "default"=>nil, 
                        "primary"=>nil, 
                        "scale"=>nil, 
                        "sql_type"=>DBI::SQL_OTHER, 
                        "nullable"=>true, 
                        "indexed"=>false, 
                        "precision"=>-1, 
                        "type_name"=>"integer[]", 
                        "unique"=>nil
                    },
                    {
                        "name"=>"baz", 
                        "default"=>nil, 
                        "primary"=>nil, 
                        "scale"=>nil, 
                        "sql_type"=>DBI::SQL_OTHER, 
                        "nullable"=>true, 
                        "indexed"=>false, 
                        "precision"=>-1, 
                        "type_name"=>"integer[]", 
                        "unique"=>nil
                    }
                ], cols
            )
        end
    end

    def test_array_parser
        # string representation
        assert_nothing_raised do
            sth = @dbh.prepare('insert into array_test (foo) values (?)')
            sth.execute('{1,2,3}')
            sth.finish
        end

        # typed representation
        # FIXME this currently fails until typed handling is fixed
        if false
            assert_nothing_raised do
                sth = @dbh.prepare('insert into array_test (foo) values (?)')
                sth.execute([1,2,3])
                sth.finish
            end
        end

        assert_nothing_raised do
            # FIXME this test should eventually have a typed result
            sth = @dbh.prepare('select foo from array_test')
            sth.execute
            assert_equal(
                [
                    # ['{1,2,3}'], FIXME uncomment me when the type handling is fixed
                    ['{1,2,3}']
                ], sth.fetch_all
            )
            sth.finish
        end
    end

    def test_array_boundaries
        # bar has a max extents of 3
        sth = @dbh.prepare('insert into array_test (bar) values (?)')

        assert_nothing_raised do
            sth.execute('{1,2,3}')
        end

        # XXX postgresql does not enforce extents on the outermost array
        assert_nothing_raised do
            sth.execute('{1,2,3,4}')
        end

        sth.finish
    end
end
