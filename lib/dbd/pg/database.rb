#
# See DBI::BaseDatabase.
#
class DBI::DBD::Pg::Database < DBI::BaseDatabase

    # type map 
    POSTGRESQL_to_XOPEN = {
          "boolean"                   => [DBI::SQL_CHAR, 1, nil],
          "character"                 => [DBI::SQL_CHAR, 1, nil],
          "char"                      => [DBI::SQL_CHAR, 1, nil],
          "real"                      => [DBI::SQL_REAL, 4, 6],
          "double precision"          => [DBI::SQL_DOUBLE, 8, 15],
          "smallint"                  => [DBI::SQL_SMALLINT, 2],
          "integer"                   => [DBI::SQL_INTEGER, 4],
          "bigint"                    => [DBI::SQL_BIGINT, 8],
          "numeric"                   => [DBI::SQL_NUMERIC, nil, nil],
          "time with time zone"       => [DBI::SQL_TIME, nil, nil],
          "timestamp with time zone"  => [DBI::SQL_TIMESTAMP, nil, nil],
          "bit varying"               => [DBI::SQL_BINARY, nil, nil], #huh??
          "character varying"         => [DBI::SQL_VARCHAR, nil, nil],
          "bit"                       => [DBI::SQL_TINYINT, nil, nil],
          "text"                      => [DBI::SQL_VARCHAR, nil, nil],
          nil                         => [DBI::SQL_OTHER, nil, nil]
    }

    attr_reader :type_map

    #
    # See DBI::BaseDatabase#new. These attributes are also supported:
    #
    # * pg_async: boolean or strings 'true' or 'false'. Indicates if we're to
    #   use PostgreSQL's asyncrohonous support. 'NonBlocking' is a synonym for
    #   this.
    # * AutoCommit: 'unchained' mode in PostgreSQL. Commits after each
    #   statement execution. 
    # * pg_client_encoding: set the encoding for the client.
    # * pg_native_binding: Boolean. Indicates whether to use libpq native
    #   binding or DBI's inline binding. Defaults to true.
    #
    def initialize(dbname, user, auth, attr)
        hash = DBI::Utils.parse_params(dbname)

        if hash['dbname'].nil? and hash['database'].nil?
            raise DBI::InterfaceError, "must specify database"
        end

        hash['options'] ||= nil
        hash['tty'] ||= ''
        hash['port'] = hash['port'].to_i unless hash['port'].nil? 

        @connection = PGconn.new(hash['host'], hash['port'], hash['options'], hash['tty'], 
                                 hash['dbname'] || hash['database'], user, auth)

        @exec_method = :exec
        @in_transaction = false

        # set attribute defaults, and look for pg_* attrs in the DSN
        @attr = { 'AutoCommit' => true, 'pg_async' => false }
        hash.each do |key, value|
            @attr[key] = value if key =~ /^pg_./
        end
        @attr.merge!(attr || {})
        if @attr['pg_async'].is_a?(String)
            case @attr['pg_async'].downcase
            when 'true'
                @attr['pg_async'] = true
            when 'false'
                @attr['pg_async'] = false
            else
                raise InterfaceError, %q{'pg_async' must be 'true' or 'false'}
            end
        end

        @attr.each { |k,v| self[k] = v} 
        @attr["pg_native_binding"] = true unless @attr.has_key? "pg_native_binding"

        @type_map = __types

        self['AutoCommit'] = true    # Postgres starts in unchained mode (AutoCommit=on) by default 

    rescue PGError => err
        raise DBI::OperationalError.new(err.message)
    end

    def disconnect
        if not @attr['AutoCommit'] and @in_transaction
            _exec("ROLLBACK")   # rollback outstanding transactions
        end
        @connection.close
    end

    def ping
        answer = _exec("SELECT 1")
        if answer
            return answer.num_tuples == 1
        else
            return false
        end
    rescue PGError
        return false
    ensure
        answer.clear if answer
    end

    def tables
        stmt = execute("SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relkind IN ('r','v') and pg_catalog.pg_table_is_visible(c.oid)")
        res = stmt.fetch_all.collect {|row| row[0]} 
        stmt.finish
        res
    end

    #
    # See DBI::BaseDatabase.
    #
    # These additional attributes are also supported:
    #
    # * nullable: true if NULL values are allowed in this column.
    # * indexed: true if this column is a part of an index.
    # * primary: true if this column is a part of a primary key.
    # * unique: true if this column is a part of a unique key.
    # * default: what will be insert if this column is left out of an insert query.
    # * array_of_type: true if this is actually an array of this type.
    #   +dbi_type+ will be the type authority if this is the case.
    #
    def columns(table)
        sql1 = %[
            select a.attname, i.indisprimary, i.indisunique
            from pg_class bc inner join pg_index i 
                on bc.oid = i.indrelid 
                inner join pg_class c 
                    on c.oid = i.indexrelid 
                    inner join pg_attribute a
                        on c.oid = a.attrelid
            where bc.relname = ?
                and bc.relkind in ('r', 'v')
                and pg_catalog.pg_table_is_visible(bc.oid);
        ]

        sql2 = %[
            SELECT a.attname, a.atttypid, a.attnotnull, a.attlen, format_type(a.atttypid, a.atttypmod) 
            FROM pg_catalog.pg_class c, pg_attribute a, pg_type t 
            WHERE a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND c.relname = ?
                AND c.relkind IN ('r','v')
            AND pg_catalog.pg_table_is_visible(c.oid)
        ]

        # by Michael Neumann (get default value)
        # corrected by Joseph McDonald
        sql3 = %[
            SELECT pg_attrdef.adsrc, pg_attribute.attname 
            FROM pg_attribute, pg_attrdef, pg_catalog.pg_class
            WHERE pg_catalog.pg_class.relname = ? AND 
            pg_attribute.attrelid = pg_catalog.pg_class.oid AND
                          pg_attrdef.adrelid = pg_catalog.pg_class.oid AND
                          pg_attrdef.adnum = pg_attribute.attnum
                          AND pg_catalog.pg_class.relkind IN ('r','v')
                          AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)
        ]

        dbh = DBI::DatabaseHandle.new(self)
        dbh.driver_name = DBI::DBD::Pg.driver_name
        indices = {}
        default_values = {}

        dbh.select_all(sql3, table) do |default, name|
            default_values[name] = default
        end

        dbh.select_all(sql1, table) do |name, primary, unique|
            indices[name] = [primary, unique]
        end

        ########## 

        ret = []
        dbh.execute(sql2, table) do |sth|
            ret = sth.collect do |row|
                name, pg_type, notnullable, len, ftype = row
                #name = row[2]
                indexed = false
                primary = nil
                unique = nil
                if indices.has_key?(name)
                    indexed = true
                    primary, unique = indices[name]
                end

                typeinfo = DBI::DBD::Pg.parse_type(ftype)
                typeinfo[:size] ||= len

                if POSTGRESQL_to_XOPEN.has_key?(typeinfo[:type])
                    sql_type = POSTGRESQL_to_XOPEN[typeinfo[:type]][0]
                else
                    sql_type = POSTGRESQL_to_XOPEN[nil][0]
                end

                row = {}
                row['name']           = name
                row['sql_type']       = sql_type
                row['type_name']      = typeinfo[:type]
                row['nullable']       = ! notnullable
                row['indexed']        = indexed
                row['primary']        = primary
                row['unique']         = unique
                row['precision']      = typeinfo[:size]
                row['scale']          = typeinfo[:decimal]
                row['default']        = default_values[name]
                row['array_of_type']  = typeinfo[:array]

                if typeinfo[:array]
                    row['dbi_type'] = 
                        DBI::DBD::Pg::Type::Array.new(
                            DBI::TypeUtil.type_name_to_module(typeinfo[:type])
                    )
                end
                row
            end # collect
        end # execute

        return ret
    end

    def prepare(statement)
        DBI::DBD::Pg::Statement.new(self, statement)
    end

    def [](attr)
        case attr
        when 'pg_client_encoding'
            @connection.client_encoding
        when 'NonBlocking'
            @attr['pg_async']
        else
            @attr[attr]
        end
    end

    def []=(attr, value)
        case attr
        when 'AutoCommit'
            if @attr['AutoCommit'] != value then
                if value    # turn AutoCommit ON
                    if @in_transaction
                        # TODO: commit outstanding transactions?
                        _exec("COMMIT")
                        @in_transaction = false
                    end
                else        # turn AutoCommit OFF
                    @in_transaction = false
                end
            end
        # value is assigned below
        when 'NonBlocking', 'pg_async'
            # booleanize input
            value = value ? true : false
            @pgexec = (value ? DBI::DBD::Pg::PgExecutorAsync : DBI::DBD::Pg::PgExecutor).new(@connection)
            # value is assigned to @attr below
        when 'pg_client_encoding'
            @connection.set_client_encoding(value)
        when 'pg_native_binding'
            @attr[attr] = value
        else
            if attr =~ /^pg_/ or attr != /_/
                raise DBI::NotSupportedError, "Option '#{attr}' not supported"
            else # option for some other driver - quitly ignore
                return
            end
        end
        @attr[attr] = value
    end

    def commit
        if @in_transaction
            _exec("COMMIT")
            @in_transaction = false
        else
            # TODO: Warn?
        end
    end

    def rollback
        if @in_transaction
            _exec("ROLLBACK")
            @in_transaction = false
        else
            # TODO: Warn?
        end
    end

    #
    # Are we in an transaction?
    #
    def in_transaction?
        @in_transaction
    end

    #
    # Forcibly initializes a new transaction.
    #
    def start_transaction
        _exec("BEGIN")
        @in_transaction = true
    end

    def _exec(sql, *parameters)
        @pgexec.exec(sql, parameters)
    end

    def _exec_prepared(stmt_name, *parameters)
        @pgexec.exec_prepared(stmt_name, parameters)
    end

    def _prepare(stmt_name, sql)
        @pgexec.prepare(stmt_name, sql)
    end

    private 

    # special quoting if value is element of an array 
    def quote_array_elements( value )
        # XXX is this method still being used?
        case value
        when Array
                        '{'+ value.collect{|v| quote_array_elements(v) }.join(',') + '}'
        when String
                        '"' + value.gsub(/\\/){ '\\\\' }.gsub(/"/){ '\\"' } + '"'
        else
            quote( value ).sub(/^'/,'').sub(/'$/,'') 
        end
    end 

    def parse_type_name(type_name)
        case type_name
        when 'bool'                      then DBI::Type::Boolean
        when 'int8', 'int4', 'int2'      then DBI::Type::Integer
        when 'varchar'                   then DBI::Type::Varchar
        when 'float4','float8'           then DBI::Type::Float
        when 'time', 'timetz'            then DBI::Type::Timestamp
        when 'timestamp', 'timestamptz'  then DBI::Type::Timestamp
        when 'date'                      then DBI::Type::Timestamp
        when 'bytea'                     then DBI::DBD::Pg::Type::ByteA
        end
    end

    #
    # Gathers the types from the postgres database and attempts to
    # locate matching DBI::Type objects for them.
    # 
    def load_type_map
        @type_map = Hash.new

        res = _exec("SELECT oid, typname, typelem FROM pg_type WHERE typtype = 'b'")

        res.each do |row|
            rowtype = parse_type_name(row["typname"])
            @type_map[row["oid"].to_i] = 
                { 
                    "type_name" => row["typname"],
                    "dbi_type" => 
                        if rowtype
                            rowtype
                        elsif row["typname"] =~ /^_/ and row["typelem"].to_i > 0 then
                            # arrays are special and have a subtype, as an
                            # oid held in the "typelem" field.
                            # Since we may not have a mapping for the
                            # subtype yet, defer by storing the typelem
                            # integer as a base type in a constructed
                            # Type::Array object. dirty, i know.
                            #
                            # These array objects will be reconstructed
                            # after all rows are processed and therefore
                            # the oid -> type mapping is complete.
                            # 
                            DBI::DBD::Pg::Type::Array.new(row["typelem"].to_i)
                        else
                            DBI::Type::Varchar
                        end
                }
        end 
        # additional conversions
        @type_map[705]  ||= DBI::Type::Varchar       # select 'hallo'
        @type_map[1114] ||= DBI::Type::Timestamp # TIMESTAMP WITHOUT TIME ZONE

        # remap array subtypes
        @type_map.each_key do |key|
            if @type_map[key]["dbi_type"].class == DBI::DBD::Pg::Type::Array
                oid = @type_map[key]["dbi_type"].base_type
                if @type_map[oid]
                    @type_map[key]["dbi_type"] = DBI::DBD::Pg::Type::Array.new(@type_map[oid]["dbi_type"])
                else
                    # punt
                    @type_map[key] = DBI::DBD::Pg::Type::Array.new(DBI::Type::Varchar)
                end
            end
        end
    end

    public

    # return the postgresql types for this session. returns an oid -> type name mapping.
    def __types(force=nil)
        load_type_map if (!@type_map or force)
        @type_map
    end

    # deprecated.
    def __types_old
        h = { } 

        _exec('select oid, typname from pg_type').each do |row|
            h[row["oid"].to_i] = row["typname"]
        end

        return h
    end

    #
    # Import a BLOB from a file.
    # 
    def __blob_import(file)
        start_transaction unless @in_transaction
        @connection.lo_import(file)
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Export a BLOB to a file.
    #
    def __blob_export(oid, file)
        start_transaction unless @in_transaction
        @connection.lo_export(oid.to_i, file)
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Create a BLOB.
    #
    def __blob_create(mode=PGconn::INV_READ)
        start_transaction unless @in_transaction
        @connection.lo_creat(mode)
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Open a BLOB.
    #
    def __blob_open(oid, mode=PGconn::INV_READ)
        start_transaction unless @in_transaction
        @connection.lo_open(oid.to_i, mode)
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Remove a BLOB.
    #
    def __blob_unlink(oid)
        start_transaction unless @in_transaction
        @connection.lo_unlink(oid.to_i)
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Read a BLOB and return the data.
    #
    def __blob_read(oid, length)
        blob = @connection.lo_open(oid.to_i, PGconn::INV_READ)

        if length.nil?
            data = @connection.lo_read(blob)
        else
            data = @connection.lo_read(blob, length)
        end

        # FIXME it doesn't like to close here either.
        # @connection.lo_close(blob)
        data
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end

    #
    # Write the value to the BLOB.
    #
    def __blob_write(oid, value)
        start_transaction unless @in_transaction
        blob = @connection.lo_open(oid.to_i, PGconn::INV_WRITE)
        res = @connection.lo_write(blob, value)
        # FIXME not sure why PG doesn't like to close here -- seems to be
        # working but we should make sure it's not eating file descriptors
        # up before release.
        # @connection.lo_close(blob)
        return res
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message)
    end

    #
    # FIXME DOCUMENT
    #
    def __set_notice_processor(proc)
        @connection.set_notice_processor proc
    rescue PGError => err
        raise DBI::DatabaseError.new(err.message) 
    end
end # Database
