require 'test/unit'
require 'fileutils'

class TestDatabase < Test::Unit::TestCase
    def test_disconnect
        assert_nil @dbh.disconnect
        assert_nil @dbh.instance_variable_get("@db")
    end

    def test_ping
        assert @dbh.ping
    end

    def setup
        config = DBDConfig.get_config['sqlite']

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
