require 'test/unit'
require 'fileutils'

DBDConfig.set_testbase(:sqlite3, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "sqlite3"
        end

        def test_base
            assert true
        end

        def setup
            config = DBDConfig.get_config['sqlite3']
            @dbh = DBI.connect('dbi:SQLite3:'+config['dbname'], nil, nil, { }) 
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlite3/up.sql")
        end

        def teardown
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite3']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
