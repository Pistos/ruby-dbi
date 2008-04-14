require 'test/unit'
require 'fileutils'

DBDConfig.set_testbase(:postgresql, Class.new(Test::Unit::TestCase) do
        
        def dbtype
            "postgresql"
        end

        def test_base
            assert_equal(@dbh.driver_name, "Pg")
            assert_kind_of(DBI::DBD::Pg::Database, @dbh.instance_variable_get(:@handle))
        end

        def setup
            config = DBDConfig.get_config['postgresql']
            @dbh = DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/postgresql/up.sql")
        end

        def teardown
            config = DBDConfig.get_config['postgresql']
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/postgresql/down.sql")
            @dbh.disconnect
        end
    end
)
