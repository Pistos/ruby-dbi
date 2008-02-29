require 'test/unit'
require 'dbi'

class TestDriver < Test::Unit::TestCase
    def test_require
        require 'dbd/SQLite'
        assert_kind_of Module, DBI
        assert_kind_of Module, DBI::DBD
        assert_kind_of Class, DBI::DBD::SQLite
        assert_kind_of Class, DBI::DBD::SQLite::Driver
        assert_kind_of Class, DBI::DBD::SQLite::Database
        assert_kind_of Class, DBI::DBD::SQLite::Statement
    end

    def test_connect
        dbh = DBI.connect("dbi:SQLite:foo.db", nil, nil, {})
        assert dbh
        assert_kind_of DBI::DatabaseHandle, dbh
    end
end
