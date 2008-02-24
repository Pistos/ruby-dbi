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

if __FILE__ == $0 then
    require 'test/unit/ui/console/testrunner'
    require 'dbi'
    Test::Unit::UI::Console::TestRunner.run(TestPostgresPing)
end
