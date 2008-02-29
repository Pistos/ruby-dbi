require 'test/unit'
require 'fileutils'

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
        config = DBDConfig.get_config['sqlite']

        # this tests DBI more than SQLite, but makes sure our chain works with it.
        dbh = DBI.connect("dbi:SQLite:" + config['dbname'], nil, nil, {})
        assert dbh
        assert_kind_of DBI::DatabaseHandle, dbh

        # first argument should be a string
        assert_raise(DBI::InterfaceError) do
            DBI::DBD::SQLite::Driver.new.connect(nil, nil, nil, { })
        end

        # that string should have some frackin' length
        assert_raise(DBI::InterfaceError) do
            DBI::DBD::SQLite::Driver.new.connect("", nil, nil, { })
        end

        # last argument should be a hash
        assert_raise(DBI::InterfaceError) do
            DBI::DBD::SQLite::Driver.new.connect(config['dbname'], nil, nil, nil)
        end

        dbh = nil
        driver = nil
        assert_nothing_raised do
            driver = DBI::DBD::SQLite::Driver.new
            dbh = driver.connect(config['dbname'], nil, nil, { })
        end

        assert_kind_of DBI::DBD::SQLite::Driver, driver
        assert_kind_of DBI::DBD::SQLite::Database, dbh

        assert !dbh.instance_variable_get("@autocommit")

        dbh = nil 
        driver = nil
        assert_nothing_raised do
            dbh = DBI::DBD::SQLite::Driver.new.connect(config['dbname'], nil, nil, { "AutoCommit" => true, "sqlite_full_column_names" => true })
        end
       
        assert dbh
        assert dbh.instance_variable_get("@attr_hash")
        assert_equal 0, dbh.instance_variable_get("@open_handles")
        assert_kind_of SQLite::Database, dbh.instance_variable_get("@db")

        assert File.exists?(config['dbname'])
    end

    def teardown
        config = DBDConfig.get_config['sqlite']
        FileUtils.rm_f(config['dbname'])
    end
end
