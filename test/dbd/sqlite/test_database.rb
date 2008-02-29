require 'test/unit'
require 'fileutils'

class TestDatabase < Test::Unit::TestCase
    def test_disconnect
        assert_nil @dbh.disconnect
        assert_nil @dbh.instance_variable_get("@db")
    end

    def test_ping
        assert @dbh.ping
        # XXX if it isn't obvious, this should be tested better. Not sure what
        # good behavior is yet.
    end

    def test_prepare
        sth = @dbh.prepare('select * from foo')

        assert sth
        assert_kind_of DBI::StatementHandle, sth

        handle = sth.instance_variable_get("@handle")


        assert_kind_of DBI::DBD::SQLite::Statement, handle
        assert handle.instance_variable_get("@dbh")
        assert_kind_of DBI::DBD::SQLite::Database, handle.instance_variable_get("@dbh")
        assert_equal(@dbh.instance_variable_get("@handle"), handle.instance_variable_get("@dbh"))
        assert_equal("select * from foo", handle.instance_variable_get("@statement"))
        assert_equal({ }, handle.instance_variable_get("@attr"))
        assert_equal([ ], handle.instance_variable_get("@params"))
        assert_nil(handle.instance_variable_get("@col_info"))
        assert_equal([ ], handle.instance_variable_get("@rows"))
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
