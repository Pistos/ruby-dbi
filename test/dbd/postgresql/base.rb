require 'test/unit'
require 'fileutils'

class PGUnitBase < Test::Unit::TestCase
    def test_base
        assert true
    end

    def setup
        config = DBDConfig.get_config['postgresql']
        system "psql #{config['dbname']} < dbd/postgresql/up.sql >>sql.log"
        @dbh = DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
    end

    def teardown
        config = DBDConfig.get_config['postgresql']
        @dbh.disconnect
        system "psql #{config['dbname']}< dbd/postgresql/down.sql >>sql.log"
    end
end

