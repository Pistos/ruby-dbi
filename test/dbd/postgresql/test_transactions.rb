require 'test/unit'

class TestPostgresTransaction < Test::Unit::TestCase
end

if __FILE__ == $0 then
    require 'test/unit/ui/console/testrunner'
    require 'dbi'
    Test::Unit::UI::Console::TestRunner.run(TestPostgresTransaction)
end
