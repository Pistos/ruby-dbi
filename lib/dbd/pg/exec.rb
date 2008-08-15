module DBI::DBD::Pg
    ################################################################
    # Convenience adaptor to hide details command execution API calls.
    # See PgExecutorAsync subclass
    class PgExecutor
        def initialize(pg_conn)
            @pg_conn = pg_conn
        end

        def exec(sql, parameters = nil)
            @pg_conn.exec(sql, parameters)
        end

        def exec_prepared(stmt_name, parameters = nil)
            @pg_conn.exec_prepared(stmt_name, parameters)
        end

        def prepare(stmt_name, sql)
            @pg_conn.prepare(stmt_name, sql)
        end
    end

    # Asynchronous implementation of PgExecutor, useful for 'green
    # thread' implementations (e.g., MRI <= 1.8.x) which would otherwise
    # suspend other threads while awaiting query results.
    #--
    # FIXME:  PQsetnonblocking + select/poll would make the exec*
    #         methods truly 'async', though this is rarely needed in
    #         practice.
    class PgExecutorAsync < PgExecutor
        def exec(sql, parameters = nil)
            @pg_conn.async_exec(sql, parameters)
        end

        def exec_prepared(stmt_name, parameters = nil)
            @pg_conn.send_query_prepared(stmt_name, parameters)
            @pg_conn.block()
            @pg_conn.get_last_result()
        end

        def prepare(stmt_name, sql)
            @pg_conn.send_prepare(stmt_name, sql)
            @pg_conn.block()
            @pg_conn.get_last_result()
        end
    end
end
