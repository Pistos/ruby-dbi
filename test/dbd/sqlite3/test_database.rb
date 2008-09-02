class TestDatabase < DBDConfig.testbase(:sqlite3)
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
    end
end
