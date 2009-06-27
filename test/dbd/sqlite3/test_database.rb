class TestDatabase < DBDConfig.testbase(:sqlite3)
    def test_database_name
        assert_nothing_raised do
            assert_equal DBDConfig.get_config[dbtype]['dbname'], @dbh.database_name
        end
    end

    def test_disconnect
        assert_nil @dbh.disconnect
        assert_nil @dbh.instance_variable_get("@db")
    end

    def test_columns
        assert_equal [
            {
                :name      => "name",
                :default   => nil,
                :nullable  => true,
                :sql_type  => 100,
                :precision => 255,
                :type_name => "varchar"
            },
            {
                :name      => "age",
                :default   => nil,
                :nullable  => true,
                :sql_type  => 4,
                :type_name => "integer"
            }
        ], @dbh.columns("names")

        assert_equal [
            {
                :name      => "name",
                :default   => nil,
                :nullable  => true,
                :sql_type  => 100,
                :precision => 255,
                :type_name => "varchar"
            },
            {
                :name      => "age",
                :default   => nil,
                :nullable  => true,
                :sql_type  => 4,
                :type_name => "integer"
            }
        ], @dbh.columns("names_defined_with_spaces")
    end
    
    def test_parse_type
      # Some tests to ensure various whitespace and case styles don't confuse parse_type.
      
      match = DBI::DBD::SQLite3.parse_type( 'VARCHAR' )
      assert_equal 'VARCHAR', match[ 1 ]
      
      match = DBI::DBD::SQLite3.parse_type( 'VARCHAR(4096)' )
      assert_equal 'VARCHAR', match[ 1 ]
      assert_equal '4096', match[ 3 ]
      
      match = DBI::DBD::SQLite3.parse_type( 'varchar(4096)' )
      assert_equal 'varchar', match[ 1 ]
      assert_equal '4096', match[ 3 ]
      
      match = DBI::DBD::SQLite3.parse_type( 'VARCHAR( 4096 )' )
      assert_equal 'VARCHAR', match[ 1 ]
      assert_equal '4096', match[ 3 ]
      
      match = DBI::DBD::SQLite3.parse_type( 'VARCHAR ( 4096 )' )
      assert_equal 'VARCHAR', match[ 1 ]
      assert_equal '4096', match[ 3 ]
      
      match = DBI::DBD::SQLite3.parse_type( 'VARCHAR (4096)' )
      assert_equal 'VARCHAR', match[ 1 ]
      assert_equal '4096', match[ 3 ]
    end
end
