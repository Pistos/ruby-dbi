class TestNewMethods < DBDConfig.testbase(:mysql)
    def test_database_name
        assert_nothing_raised do
            assert_equal DBDConfig.get_config[dbtype]['dbname'], @dbh.database_name
        end
    end
end
