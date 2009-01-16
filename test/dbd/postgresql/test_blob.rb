class TestPostgresBlob < DBDConfig.testbase(:postgresql)
    DATA = "this is my new binary object"

    def test_insert
        assert @dbh
        assert @dbh.ping

        # test with DBI::Binary
        assert_equal 1, @dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)", "test", DBI::Binary.new(DATA))

        # test with blob_create directly
        blob = @dbh.func(:blob_create, PGconn::INV_WRITE)
        assert blob
        assert @dbh.func(:blob_write, blob, DATA)
        assert_equal 1, @dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)", "test (2)", blob)

        # test with blob_import directly
        File.open('/tmp/pg_dbi_import_test', 'w') { |f| f << DATA }
        blob = @dbh.func(:blob_import, '/tmp/pg_dbi_import_test')
        assert blob
        assert_equal 1, @dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)", "test (2)", blob)

        index = 0
        @dbh.select_all("SELECT name, data FROM blob_test") do |name, data|
            index += 1
            assert_equal DATA, @dbh.func(:blob_read, data, DATA.length)
            @dbh.func(:blob_export, data, '/tmp/pg_dbi_read_test')
            assert_equal DATA, File.read('/tmp/pg_dbi_read_test')
        end

        assert_equal 3, index

        File.unlink("/tmp/pg_dbi_read_test")
        File.unlink("/tmp/pg_dbi_import_test")
    end
end
