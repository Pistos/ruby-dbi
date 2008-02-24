require 'test/unit'

class TestPostgresPing < Test::Unit::TestCase
    def test_ping
        dbh = DBI.connect("dbi:Pg:rubytest:127.0.0.1", "erikh", "monkeys")
        assert dbh
        assert dbh.ping
        dbh.disconnect
        assert_raise(DBI::InterfaceError) { dbh.ping }
    end
end
