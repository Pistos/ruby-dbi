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
        # this tests DBI more than SQLite, but makes sure our chain works with it.
        dbh = DBI.connect("dbi:SQLite:foo.db", nil, nil, {})
        assert dbh
        assert_kind_of DBI::DatabaseHandle, dbh

        # first argument should be a string
        assert_raise(DBI::InterfaceError) do
            DBI::DBD::SQLite::Driver.new.connect(nil, nil, nil, { })
        end

        # last argument should be a hash
        assert_raise(DBI::InterfaceError) do
            DBI::DBD::SQLite::Driver.new.connect("", nil, nil, nil)
        end
    end
end
