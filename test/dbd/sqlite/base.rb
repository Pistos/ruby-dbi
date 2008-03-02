require 'test/unit'
require 'fileutils'

class SQLiteUnitBase < Test::Unit::TestCase
    def test_base
        assert true
    end

    def setup
        config = DBDConfig.get_config['sqlite']

        system("sqlite #{config['dbname']} < dbd/sqlite/up.sql");

        # this will not be used in all tests
        @dbh = DBI.connect('dbi:SQLite:'+config['dbname'], nil, nil, { }) 
    end

    def teardown
        # XXX obviously, this comes with its problems as some of this is being
        # tested here.
        @dbh.disconnect if @dbh.connected?
        config = DBDConfig.get_config['sqlite']
        FileUtils.rm_f(config['dbname'])
    end
end
