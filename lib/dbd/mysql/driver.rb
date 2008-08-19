module DBI::DBD::Mysql
    #
    # Models the DBI::BaseDriver API to create DBI::DriverHandle objects.
    #
    class Driver < DBI::BaseDriver
        include Util

        def initialize
            super("0.4.0")
        end

        def default_user
            ['', nil]
        end

        #
        # Parameters in the dbname as follows:
        #
        # * host: host to connect to
        # * port: port to connect to
        # * socket: connect to a specific unix socket instead of a TCP socket.
        # * flag: an OR'd collection of flags to pass to the lower-level
        #   connection attempt.
        # * mysql_read_default_file: FIXME
        # * mysql_read_default_group: FIXME
        # * mysql_compression: FIXME
        # * mysql_local_infile: FIXME
        # * mysql_client_found_rows: FIXME boolean, modifies the 'flag'
        #   setting above.
        def connect(dbname, user, auth, attr)
            # connect to database server
            hash = DBI::Utils.parse_params(dbname)

            hash['host'] ||= 'localhost'

            # these two connection parameters should be passed as numbers
            hash['port'] = hash['port'].to_i unless hash['port'].nil?
            hash['flag'] = hash['flag'].nil? ? 0 : hash['flag'] = hash['flag'].to_i

            handle = ::Mysql.init

            # Look for options in connect string to be handled
            # through mysql_options() before connecting
            !hash['mysql_read_default_file'].nil? and
            handle.options(::Mysql::READ_DEFAULT_FILE,
                           hash['mysql_read_default_file'])
            !hash['mysql_read_default_group'].nil? and
            handle.options(::Mysql::READ_DEFAULT_GROUP,
                           hash['mysql_read_default_group'])
            # The following options can be handled either using mysql_options()
            # or in the flag argument to connect().
            hash['mysql_compression'].to_i != 0 and
            handle.options(::Mysql::OPT_COMPRESS, nil)
            hash['mysql_local_infile'].to_i != 0 and
            handle.options(::Mysql::OPT_LOCAL_INFILE, true)

            # Look for options to be handled in the flags argument to connect()
            if !hash['mysql_client_found_rows'].nil?
                if hash['mysql_client_found_rows'].to_i != 0
                    hash['flag'] |= ::Mysql::CLIENT_FOUND_ROWS
                else
                    hash['flag'] &= ~::Mysql::CLIENT_FOUND_ROWS
                end
            end

            handle.connect(hash['host'], user, auth, hash['database'], hash['port'], hash['socket'], hash['flag'])

            return Database.new(handle, attr)
        rescue MyError => err
            error(err)
        end

        def data_sources
            handle = ::Mysql.new
            res = handle.list_dbs.collect {|db| "dbi:Mysql:database=#{db}" }
            handle.close
            return res
        rescue MyError => err
            error(err)
        end

        #--
        # Driver-specific functions ------------------------------------------------
        #++

        public

        def __createdb(db, host, user, password, port=nil, sock=nil, flag=nil)
            handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
            begin
                handle.create_db(db)
            ensure
                handle.close if handle
            end
        end

        def __dropdb(db, host, user, password, port=nil, sock=nil, flag=nil)
            handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
            begin
                handle.drop_db(db)
            ensure
                handle.close if handle
            end
        end

        def __shutdown(host, user, password, port=nil, sock=nil, flag=nil)
            handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
            begin
                handle.shutdown
            ensure
                handle.close if handle
            end
        end

        def __reload(host, user, password, port=nil, sock=nil, flag=nil)
            handle = ::Mysql.connect(host, user, password, nil, port, sock, flag)
            begin
                handle.reload
            ensure
                handle.close if handle
            end
        end

    end # class Driver
end
