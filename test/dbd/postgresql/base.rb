require 'test/unit'
require 'fileutils'

DBDConfig.set_testbase(:postgresql, Class.new(Test::Unit::TestCase) do
        
        def dbtype
            "postgresql"
        end

        def test_base
            assert true
        end

        def setup
            config = DBDConfig.get_config['postgresql']
            system "psql #{config['dbname']} < dbd/postgresql/up.sql >>sql.log 2>&1"
            @dbh = DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
        end

        def teardown
            config = DBDConfig.get_config['postgresql']
            @dbh.disconnect
            system "psql #{config['dbname']}< dbd/postgresql/down.sql >>sql.log 2>&1"
        end
    end
)
