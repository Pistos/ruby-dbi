class TestODBCStatement < DBDConfig.testbase(:odbc)
    def test_column_info
        sth = nil
        
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names")
            sth.execute
        end

        assert_kind_of Array, sth.column_info 
        assert_kind_of DBI::ColumnInfo, sth.column_info[0]
        assert_kind_of DBI::ColumnInfo, sth.column_info[1]
        assert_equal [
            {
                :table=>"", 
                :precision=>255, 
                :searchable=>true, 
                :name=>"name", 
                :unsigned=>true, 
                :length=>255, 
                :sql_type=>4294967295, 
                :scale=>0, 
                :nullable=>true, 
                :type_name=>nil
            },
            {
                :table=>"", 
                :precision=>10, 
                :searchable=>true, 
                :name=>"age", 
                :unsigned=>false, 
                :length=>4, 
                :sql_type=>4, 
                :scale=>0, 
                :nullable=>true, 
                :type_name=>"INTEGER"
            }
        ], sth.column_info

        sth.finish
    end
end
