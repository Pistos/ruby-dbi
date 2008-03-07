class TestMysqlPatches < DBDConfig.testbase(:mysql)
    def test_exception_on_aggregate
        assert_nothing_raised do
            sth = @dbh.prepare("select sum(age) from names")
            sth.execute
            row = sth.fetch 
            # FIXME: this is BROKEN - should not be a string, should be fixnum
            assert_equal("70", row[0])
            sth.finish
        end
    end
end
