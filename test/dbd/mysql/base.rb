DBDConfig.set_testbase(:mysql, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "mysql"
        end

        def test_base
            assert_equal(@dbh.driver_name, "Mysql")
        end

        def setup
            config = DBDConfig.get_config["mysql"]
            @dbh = DBI.connect("dbi:Mysql:"+config["dbname"], config["username"], config["password"], { })
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/mysql/up.sql")
        end

        def teardown
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/mysql/down.sql")
            @dbh.disconnect
        end
    end
)
