require 'test/unit'
require 'fileutils'

class TestStatement < Test::Unit::TestCase
    def test_constructor
        sth = DBI::DBD::SQLite::Statement.new("select * from foo", @dbh.instance_variable_get("@handle"))

        assert_kind_of DBI::DBD::SQLite::Statement, sth
        assert sth.instance_variable_get("@dbh")
        assert_kind_of DBI::DBD::SQLite::Database, sth.instance_variable_get("@dbh")
        assert_equal(@dbh.instance_variable_get("@handle"), sth.instance_variable_get("@dbh"))
        assert_equal("select * from foo", sth.instance_variable_get("@statement"))
        assert_equal({ }, sth.instance_variable_get("@attr"))
        assert_equal([ ], sth.instance_variable_get("@params"))
        assert_nil(sth.instance_variable_get("@col_info"))
        assert_equal([ ], sth.instance_variable_get("@rows"))
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
