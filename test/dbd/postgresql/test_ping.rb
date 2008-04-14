class TestPostgresPing < DBDConfig.testbase(:postgresql)
    def test_ping
        config = DBDConfig.get_config['postgresql']
        dbh = DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
        assert dbh
        assert dbh.ping
        dbh.disconnect
        assert_raise(DBI::InterfaceError) { dbh.ping }
    end
end
