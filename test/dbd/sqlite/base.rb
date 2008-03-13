require 'test/unit'
require 'fileutils'

DBDConfig.set_testbase(:sqlite, Class.new(Test::Unit::TestCase) do

        def dbtype
            "sqlite"
        end

        def test_base
            assert true
        end

        def setup
            config = DBDConfig.get_config['sqlite']
            @dbh = DBI.connect('dbi:SQLite:'+config['dbname'], nil, nil, { }) 
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlite/up.sql")
        end

        def teardown
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
