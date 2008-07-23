class TestSQLiteDatabase < DBDConfig.testbase(:sqlite)
    def test_disconnect
        assert_nil @dbh.disconnect
        assert_nil @dbh.instance_variable_get("@db")
    end
    
    def test_columns
        assert_equal [
            {
                :name       => "name",
                :default    => nil,
                :nullable   => true,
                :precision  => 255,
                :type_name  => "varchar"
            },
            {
                :name       => "age",
                :default    => nil,
                :nullable   => true,
                :type_name  => "integer"
            }
        ], @dbh.columns("names")  
    end
end
