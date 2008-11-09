DBDConfig.set_testbase(:mysql, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "mysql"
        end

        def test_base
            assert_equal(@dbh.driver_name, "Mysql")
            assert_kind_of(DBI::DBD::Mysql::Database, @dbh.instance_variable_get(:@handle))
        end

        def set_base_dbh
            config = DBDConfig.get_config["mysql"]
            @dbh = DBI.connect("dbi:Mysql:"+config["dbname"], config["username"], config["password"], { })
        end

        def setup
            set_base_dbh
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/mysql/up.sql")
        end

        def teardown
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/mysql/down.sql")
            @dbh.disconnect
        end
    end
)
