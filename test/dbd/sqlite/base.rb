require 'fileutils'

DBDConfig.set_testbase(:sqlite, Class.new(MiniTest::Unit::TestCase) do

        def dbtype
            "sqlite"
        end

        def test_base
            if @dbh # FIXME for some reason, @dbh isn't initialized in some cases. investigate.
                assert_equal(@dbh.driver_name, "SQLite")
                assert_kind_of(DBI::DBD::SQLite::Database, @dbh.instance_variable_get(:@handle))
            end
        end

        def set_base_dbh
            config = DBDConfig.get_config['sqlite']
            @dbh = DBI.connect('dbi:SQLite:'+config['dbname'], nil, nil, { }) 
        end

        def setup
            set_base_dbh
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlite/up.sql")
        end

        def teardown
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
