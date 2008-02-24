require 'test/unit'

class TestPostgresBlob < Test::Unit::TestCase
    DATA = "this is my new binary object"

    def test_insert
        assert @dbh
        assert @dbh.ping

        # FIXME
        #
        # Figure out what's causing the "truncated during write" error, is it
        # the postgres module, or our DBD?
        #
        assert_equal 1, @dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)", "test", DBI::Binary.new(DATA))

        blob = @dbh.func(:blob_create, PGlarge::INV_WRITE)

        assert blob

        assert blob.open
        assert blob.write(DATA)

        assert_equal 1, @dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)", "test (2)", blob.oid)

        assert blob.close

        @dbh.select_all("SELECT name, data FROM blob_test") do |name, data|
            assert_equal DATA, dbh.func(:blob_read, data)
            assert dbh.func(:blob_export, data, '/tmp/dbitest')
            assert_equal DATA, File.readlines('/tmp/dbitest').to_s

            blob = dbh.func(:blob_open, data, PGlarge::INV_READ)  
            assert blob
            assert blob.open
            assert_equal DATA, blob.read
            assert blob.close
        end
    end

    def setup
        system "psql rubytest < dump.sql >>sql.log"
        @dbh = DBI.connect("dbi:Pg:rubytest", "erikh", "monkeys")
    end

    def teardown
        @dbh.disconnect
        system "psql rubytest < drop_tables.sql >>sql.log"
    end
end

if __FILE__ == $0 then
    require 'test/unit/ui/console/testrunner'
    require 'dbi'
    Test::Unit::UI::Console::TestRunner.run(TestPostgresBlob)
end
