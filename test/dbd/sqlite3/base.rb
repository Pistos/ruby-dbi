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

            system("sqlite3 #{config['dbname']} < dbd/sqlite3/up.sql");

            # this will not be used in all tests
            @dbh = DBI.connect('dbi:SQLite3:'+config['dbname'], nil, nil, { }) 
        end

        def teardown
            # XXX obviously, this comes with its problems as some of this is being
            # tested here.
            @dbh.disconnect if @dbh.connected?
            config = DBDConfig.get_config['sqlite3']
            FileUtils.rm_f(config['dbname'])
        end
    end
)
