@class = Class.new(DBDConfig.testbase(DBDConfig.current_dbtype)) do
    def test_time
        sth = nil
        t = nil
        assert_nothing_raised do
            sth = @dbh.prepare("insert into time_test (mytime) values (?)")
            t = Time.now
            sth.execute(t)
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from time_test")
            sth.execute
            row = sth.fetch
            assert_kind_of DBI::Time, row[0]
            assert_equal t.hour, row[0].hour
            assert_equal t.min, row[0].minute
            assert_equal t.sec, row[0].second
            sth.finish
        end
    end

    def test_boolean_return
        sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into boolean_test (num, mybool) values (?, ?)")
            sth.execute(1, true)
            sth.execute(2, false)
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from boolean_test order by num")
            sth.execute

            pairs = sth.fetch_all

            assert_equal(
                [
                             [1, true],
                             [2, false],
                ], pairs
            )

            sth.finish
        end
    end

    def skip_bit
        # FIXME this test fails because DBI's type system blows goats.
       sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into bit_test (mybit) values (?)")
            sth.bind_param(1, 0, DBI::SQL_TINYINT)
            sth.execute
#             if dbtype == "postgresql"
#                 sth.execute("0")
#             else
#                 sth.execute(0)
#             end
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from bit_test")
            sth.execute
            row = sth.fetch
            sth.finish

            assert_equal [0], row
        end
    end
end
