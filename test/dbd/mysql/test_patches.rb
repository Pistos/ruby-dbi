class TestMysqlPatches < DBDConfig.testbase(:mysql)
    def test_exception_on_aggregate
        assert_nothing_raised do
            sth = @dbh.prepare("select sum(age) from names")
            sth.execute
            row = sth.fetch 
            assert_equal(70.0, row[0])
            sth.finish

            sth = @dbh.prepare("select count(*) from names")
            sth.execute
            assert_equal([3], sth.fetch)
            sth.finish
        end
    end

    def test_timestamps
        timestamp = "04-06-1978 06:00:00"
        datestamp = "04-06-1978"
        date  = Date.strptime(datestamp, "%m-%d-%Y")
        stamp = DateTime.strptime(timestamp, "%m-%d-%Y %H:%M:%S")
        assert_nothing_raised do
            @sth = @dbh.prepare("insert into db_specific_types_test (ts) values (?)")
            @sth.execute(stamp)
            @sth.finish
        end

        assert_nothing_raised do
            @sth = @dbh.prepare("select ts from db_specific_types_test where ts is not null")
            @sth.execute

            newstamp = @sth.fetch[0]

            assert_equal(newstamp, stamp)
            assert_equal(newstamp.strftime("%m-%d-%Y %H:%M:%S"), timestamp)
            @sth.finish
        end

        assert_nothing_raised do
            @sth = @dbh.prepare("insert into db_specific_types_test (dt) values (?)")
            @sth.execute(date)
            @sth.finish
        end

        assert_nothing_raised do
            @sth = @dbh.prepare("select dt from db_specific_types_test where dt is not null")
            @sth.execute
            
            newdate = @sth.fetch[0]

            assert_equal(newdate, date)
            assert_equal(newdate.strftime("%m-%d-%Y"), datestamp)
            @sth.finish
        end
    end

    # FIXME when the spec is more solid, this should be in the general tests
    def test_columns
        assert_nothing_raised do
            assert_equal [
                {
                    :name =>"foo",
                    :default =>"1",
                    :primary =>true,
                    :scale =>nil,
                    :sql_type =>4,
                    :nullable =>false,
                    :indexed =>true,
                    :precision =>11,
                    :type_name =>"int",
                    :unique =>true
                }
            ], @dbh.columns("field_types_test")
        end

        assert_nothing_raised do
            sth = @dbh.prepare("insert into field_types_test (foo) values (?)")
            sth.execute(2)
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from field_types_test")
            sth.execute
            row = sth.fetch
            columns = sth.column_info
            sth.finish

            assert_equal [2], row
            assert_equal [
                {
                    :dbi_type => DBI::Type::Integer,
                    :name =>"foo",
                    :mysql_type_name =>"INT",
                    :mysql_max_length =>1,
                    :primary =>true,
                    :scale =>0,
                    :mysql_flags =>49155,
                    :sql_type =>4,
                    :nullable =>false,
                    :mysql_type =>3,
                    :indexed =>true,
                    :mysql_length =>11,
                    :precision =>11,
                    :type_name =>"INTEGER",
                    :unique =>true
                }
            ], columns
        end
    end
end
