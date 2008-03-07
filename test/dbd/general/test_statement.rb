@class = Class.new(DBDConfig.testbase(DBDConfig.current_dbtype)) do
    def test_rows
        sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Bill", 22);
        end

        assert 1, sth.rows

        sth.finish
        sth = nil

        assert_nothing_raised do
            sth = @dbh.prepare("delete from names where name = ?")
            sth.execute("Bill");
        end

        assert 1, sth.rows

        sth.finish
    end

    def test_execute
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names")
            sth.execute
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("select * from names where name = ?")
            sth.execute("Bob")
            sth.finish
        end

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Bill", 22);
            sth.finish
        end
    end

    def test_execute_with_transactions
        @dbh["AutoCommit"] = false 
        config = DBDConfig.get_config['sqlite3']

        # rollback 1 (the right way)
        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end
        assert_equal ["Billy", 23 ], sth2.fetch
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.rollback }

        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_nil sth.fetch
        sth.finish
       
        # rollback 2 (without closing statements first)

        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end

        assert_equal ["Billy", 23], sth2.fetch

        # FIXME some throw here, some don't. we should probably normalize this
        @dbh.rollback rescue true
        
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.rollback }
        
        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_nil sth.fetch
        sth.finish

        # commit

        sth = nil
        sth2 = nil

        assert_nothing_raised do
            sth = @dbh.prepare("insert into names (name, age) values (?, ?)")
            sth.execute("Billy", 23)
            sth2 = @dbh.prepare("select * from names where name = ?")
            sth2.execute("Billy")
        end
        assert_equal ["Billy", 23 ], sth2.fetch
        sth2.finish
        sth.finish
        assert_nothing_raised { @dbh.commit }

        sth = @dbh.prepare("select * from names where name = ?")
        sth.execute("Billy")
        assert_equal ["Billy", 23 ], sth.fetch
        sth.finish
    end

    def test_fetch
        sth = nil
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names order by age")
            sth.execute
        end

        # this tests that we're getting the rows in the right order,
        # and that the types are being converted. 
        assert_equal ["Joe", 19], sth.fetch
        assert_equal ["Bob", 21], sth.fetch
        assert_equal ["Jim", 30], sth.fetch
        assert_nil sth.fetch

        sth.finish
    end
end
