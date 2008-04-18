require 'test/unit'
require 'fileutils'

DBDConfig.set_testbase(:sqlite3, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "sqlite3"
        end

        def test_base
            assert_equal(@dbh.driver_name, "SQLite3")
            assert_kind_of(DBI::DBD::SQLite3::Database, @dbh.instance_variable_get(:@handle))
        end

        def setup
            config = DBDConfig.get_config['sqlite3']
            @dbh = DBI.connect('dbi:SQLite3:'+config['dbname'], nil, nil, { }) 
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlite3/up.sql")
        end

        def teardown
            @sth.finish if @sth && !@sth.finished?
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite3']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
