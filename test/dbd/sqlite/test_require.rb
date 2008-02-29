require 'test/unit'
require 'dbi'

class TestRequire < Test::Unit::TestCase
    def test_require
        assert(require('dbd/SQLite'))
        DBI.connect("dbi:SQLite:foo.db", nil, nil, {})
    end
end
