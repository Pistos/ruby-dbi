class TestODBCPing < DBDConfig.testbase(:odbc)
    def test_database_name
        #
        # NOTE this test will fail if the database is not named "rubytest". I
        # don't think there's a good way to get around this, so I've set it to
        # what I typically use in my odbc.ini. - erikh
        #
        assert_nothing_raised do
            assert_equal "rubytest", @dbh.database_name
        end
    end
end
