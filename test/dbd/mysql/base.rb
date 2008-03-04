DBDConfig.set_testbase(:mysql, Class.new(Test::Unit::TestCase) do
        def test_base
            assert true
        end

        def setup
            config = DBDConfig.get_config["mysql"]
            system("mysql5 -u #{config['username']} #{config['password'] ? "-p "+config['password'] : ''} #{config['dbname']} < dbd/mysql/up.sql")
            @dbh = DBI.connect("dbi:mysql:"+config["dbname"], config["username"], config["password"], { })
        end

        def teardown
            @dbh.disconnect
            config = DBDConfig.get_config["mysql"]
            system("mysql5 -u #{config['username']} #{config['password'] ? "-p "+config['password'] : ''} #{config['dbname']} < dbd/mysql/down.sql")
        end
    end
)
