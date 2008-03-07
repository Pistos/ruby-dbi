Class.new(DBDConfig.testbase(DBDConfig.current_dbtype)) do
    def test_ping
        assert @dbh.ping
        # XXX if it isn't obvious, this should be tested better. Not sure what
        # good behavior is yet.
    end

    def test_prepare
        sth = @dbh.prepare('select * from names')

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
        assert_equal %w(blob_test boolean_test names view_names), @dbh.tables.sort
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
end
