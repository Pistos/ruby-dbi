require File.join(File.dirname(__FILE__), 'base')

class TestPostgresPing < PGUnitBase
    def test_ping
        config = DBDConfig.get_config['postgresql']
        dbh = DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
        assert dbh
        assert dbh.ping
        dbh.disconnect
        assert_raise(DBI::InterfaceError) { dbh.ping }
    end

    def setup
    end

    def teardown
    end
end
