class TestMySQLBlob < DBDConfig.testbase(:mysql)
    def test_blob_round_trip
        data =(0..255).collect{|n| n.chr}.join("")
        sql = "INSERT INTO blob_test (name, data) VALUES (?, ?)"

        @dbh.do(sql, 'test1', DBI::Binary.new(data)) 
        @dbh.do(sql, 'test2', data) 

        @dbh.prepare(sql) do |sth|
            sth.execute('test3', DBI::Binary.new(data))
            sth.execute('test4', data)
        end

        @dbh.select_all("SELECT name, data FROM blob_test") do |name, fetch_data|
            assert_equal fetch_data, data
        end
    end
end
