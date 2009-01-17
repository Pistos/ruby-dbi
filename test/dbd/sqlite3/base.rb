require 'fileutils'

DBDConfig.set_testbase(:sqlite3, Class.new(Test::Unit::TestCase) do
        def dbtype 
            "sqlite3"
        end

        def test_base
            if @dbh # FIXME for some reason, @dbh isn't initialized in some cases. investigate.
                assert_equal(@dbh.driver_name, "SQLite3")
                assert_kind_of(DBI::DBD::SQLite3::Database, @dbh.instance_variable_get(:@handle))
            end
        end

        def set_base_dbh
            config = DBDConfig.get_config['sqlite3']
            @dbh = DBI.connect('dbi:SQLite3:'+config['dbname'], nil, nil, { }) 
        end

        def setup
            set_base_dbh
            DBDConfig.inject_sql(@dbh, dbtype, "dbd/sqlite3/up.sql")
        end

        def teardown
            @sth.finish if(@sth && !@sth.finished?)
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite3']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
