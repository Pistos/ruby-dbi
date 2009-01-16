class TestDriver < DBDConfig.testbase(:sqlite3)
    def test_require
        require 'dbd/SQLite3'
        assert_kind_of Module, DBI
        assert_kind_of Module, DBI::DBD
        assert_kind_of Module, DBI::DBD::SQLite3
        assert_kind_of Class, DBI::DBD::SQLite3::Driver
        assert_kind_of Class, DBI::DBD::SQLite3::Database
        assert_kind_of Class, DBI::DBD::SQLite3::Statement
    end

    def test_connect
        config = DBDConfig.get_config['sqlite3']

        # this tests DBI more than SQLite, but makes sure our chain works with it.
        dbh = DBI.connect("dbi:SQLite3:" + config['dbname'], nil, nil, {})
        assert dbh
        assert_kind_of DBI::DatabaseHandle, dbh

        # first argument should be a string
        assert_raises(DBI::InterfaceError) do
            DBI::DBD::SQLite3::Driver.new.connect(nil, nil, nil, { })
        end

        # that string should have some frackin' length
        assert_raises(DBI::InterfaceError) do
            DBI::DBD::SQLite3::Driver.new.connect("", nil, nil, { })
        end

        # last argument should be a hash
        assert_raises(DBI::InterfaceError) do
            DBI::DBD::SQLite3::Driver.new.connect(config['dbname'], nil, nil, nil)
        end

        dbh = nil
        driver = nil
        assert_nothing_raised do
            driver = DBI::DBD::SQLite3::Driver.new
            dbh = driver.connect(config['dbname'], nil, nil, { })
        end

        assert_kind_of DBI::DBD::SQLite3::Driver, driver
        assert_kind_of DBI::DBD::SQLite3::Database, dbh

        assert !dbh.instance_variable_get("@autocommit")

        dbh = nil 
        driver = nil
        assert_nothing_raised do
            dbh = DBI::DBD::SQLite3::Driver.new.connect(config['dbname'], nil, nil, { "AutoCommit" => true, "sqlite_full_column_names" => true })
        end
       
        assert dbh
        assert dbh.instance_variable_get("@attr")
        assert_kind_of SQLite3::Database, dbh.instance_variable_get("@db")

        assert File.exists?(config['dbname'])
    end

    def setup
    end

    def teardown
        config = DBDConfig.get_config['sqlite3']
        FileUtils.rm_f(config['dbname'])
    end
end
