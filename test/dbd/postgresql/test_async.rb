class TestPostgresAsync < DBDConfig.testbase(:postgresql)
    def get_dbh
        config = DBDConfig.get_config['postgresql']
        DBI.connect("dbi:Pg:#{config['dbname']}", config['username'], config['password'])
    end

    def get_db_info
        config = DBDConfig.get_config['postgresql']
        dsn  = "dbi:Pg:database=#{config['dbname']}"
        user = config['username']
        pass = config['password']

        return [dsn, user, pass]
    end

    def test_async_default
        dsn, user, pass = get_db_info

        DBI.connect(dsn, user, pass) do |dbh|
            assert_equal false, dbh['pg_async']
            assert_equal false, dbh['NonBlocking']
            dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                assert_equal false, sth['pg_async']
                assert_equal false, sth['NonBlocking']
            end
        end
    end

    def test_async_dsn_enable
        dsn, user, pass = get_db_info

        for enable in ['true', 'TRUE', 'tRuE']
            DBI.connect(dsn + ";pg_async=#{enable}", user, pass) do |dbh|
                assert_equal true, dbh['pg_async']
                assert_equal true, dbh['NonBlocking']
                dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                    assert_equal true, sth['pg_async']
                    assert_equal true, sth['NonBlocking']
                end
            end
        end
    end

    def test_async_attr_enable
        dsn, user, pass = get_db_info

        for enable in ['true', 'TRUE', 'tRuE']
            DBI.connect(dsn, user, pass, { 'pg_async' => enable } ) do |dbh|
                assert_equal true, dbh['pg_async']
                assert_equal true, dbh['NonBlocking']
                dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                    assert_equal true, sth['pg_async']
                    assert_equal true, sth['NonBlocking']
                end
            end
        end
    end

    def test_async_dsn_disable
        dsn, user, pass = get_db_info

        for disable in ['false', 'FALSE', 'fAlSe']
            DBI.connect(dsn + ";pg_async=#{disable}", user, pass) do |dbh|
                assert_equal false, dbh['pg_async']
                assert_equal false, dbh['NonBlocking']
                dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                    assert_equal false, sth['pg_async']
                    assert_equal false, sth['NonBlocking']
                end
            end
        end
    end

    def test_async_attr_disable
        dsn, user, pass = get_db_info

        for disable in ['false', 'FALSE', 'fAlSe']
            DBI.connect(dsn, user, pass, { 'pg_async' => disable }) do |dbh|
                assert_equal false, dbh['pg_async']
                assert_equal false, dbh['NonBlocking']
                dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                    assert_equal false, sth['pg_async']
                    assert_equal false, sth['NonBlocking']
                end
            end
        end
    end

    def test_manual_enable
        dsn, user, pass = get_db_info

        DBI.connect(dsn, user, pass) do |dbh|
            dbh['pg_async'] = true
            assert_equal true, dbh['pg_async']
            assert_equal true, dbh['NonBlocking']
            dbh.prepare('SELECT 1') do |sth|      # Statement inherits
                assert_equal true, sth['pg_async']
                assert_equal true, sth['NonBlocking']
            end
        end
    end

    def test_async_commands
        dsn, user, pass = get_db_info

        DBI.connect(dsn + ";pg_async=true", user, pass) do |dbh|
            assert_equal true, dbh['pg_async']
            assert_equal true, dbh['NonBlocking']
            ret = dbh.select_all('SELECT 1')
            assert_equal [[1]], ret

            ret = dbh.select_all(%q{SELECT 1 WHERE 'foo' = ?}, 'bar')
            assert_equal [], ret

            dbh.prepare(%q{SELECT 1 WHERE 'foo' = ?}) do |sth|
                sth.execute('bar')
                assert_equal [], sth.fetch_all
            end
        end
    end
end
