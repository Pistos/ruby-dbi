require 'test/unit'
require 'fileutils'

class TestDatabase < Test::Unit::TestCase
    def test_disconnect
        assert_nil @dbh.disconnect
        assert_nil @dbh.instance_variable_get("@db")
    end

    def test_ping
        assert @dbh.ping
        # XXX if it isn't obvious, this should be tested better. Not sure what
        # good behavior is yet.
    end

    def test_prepare
        sth = @dbh.prepare('select * from foo')

        assert sth
        assert_kind_of DBI::StatementHandle, sth

        sth.finish
    end

    def test_do
        assert_equal 1, @dbh.do("insert into names (name, age) values (?, ?)", "Billy", 21)
        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_equal ["Billy", 21], sth.fetch
        sth.finish
    end

    def test_tables
        assert_equal ["names"], @dbh.tables
    end

    def test_columns
        assert_equal [
            {
                "name"      => "name",
                "default"   => nil,
                "nullable"  => false,
                "precision" => 255,
                "type_name" => "varchar"
            },
            {
                "name"      => "age",
                "default"   => nil,
                "nullable"  => false,
                "type_name" => "integer"
            }
        ], @dbh.columns("names")  
    end

    def test_attrs
        # test defaults
        assert @dbh["AutoCommit"] # should be true

        # test setting
        assert !(@dbh["AutoCommit"] = false)
        assert !@dbh["AutoCommit"]

        # test committing an outstanding transaction
        
        sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
        sth.execute("Billy", 22)
        sth.finish

        assert @dbh["AutoCommit"] = true # should commit at this point
        
        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_equal [ "Billy", 22 ], sth.fetch
        sth.finish
    end

    def setup
        config = DBDConfig.get_config['sqlite']

        system("sqlite #{config['dbname']} < dbd/sqlite/up.sql");

        # this will not be used in all tests
        @dbh = DBI.connect('dbi:SQLite:'+config['dbname'], nil, nil, { }) 
    end

    def teardown
        # XXX obviously, this comes with its problems as some of this is being
        # tested here.
        @dbh.disconnect if @dbh.connected?
        config = DBDConfig.get_config['sqlite']
        FileUtils.rm_f(config['dbname'])
    end
end
