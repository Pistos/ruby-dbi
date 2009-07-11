#
# See DBI::BaseDatabase.
#
class DBI::DBD::SQLite3::Database < DBI::BaseDatabase
    #
    # Constructor. Valid attributes:
    #
    # * AutoCommit: Commit after every statement execution.
    #
    # The following attributes go directly to the low-level SQLite3 driver. 
    # Please consult it's documentation for more information.
    #
    # * auto_vacuum
    # * cache_size
    # * default_cache_size
    # * default_synchronous
    # * default_temp_store
    # * full_column_names
    # * synchronous
    # * temp_store
    # * type_translation
    #
    def initialize(dbname, attr)
        @db = ::SQLite3::Database.new(dbname)

        @db.type_translation = false

        @attr = {'AutoCommit' => true}
        if attr then
            attr.each_pair do |key, value|
                begin
                    self[key] = value
                rescue DBI::NotSupportedError
                end
            end
        end
        __generate_attr__
    end

    def disconnect()
        @db.rollback if @db.transaction_active?
        @db.close
    end

    def prepare(statement)
        DBI::DBD::SQLite3::Statement.new(statement, @db)
    end

    def database_name
        st = DBI::DBD::SQLite3::Statement.new('PRAGMA database_list', @db)
        st.execute
        row = st.fetch
        st.finish

        return row[2]
    end

    def ping()
        not @db.closed?
    end

    def commit()
        if @db.transaction_active?
            @db.commit
            @db.transaction
        else
            raise DBI::ProgrammingError.new("No active transaction.")
        end
    end

    #
    # See DBI::BaseDatabase#rollback.
    #
    # If all statements were not closed before the rollback occurs, a
    # DBI::Warning may be raised if the database encounters an error because of
    # it.
    #
    # This method will also raise DBI::ProgrammingError if not in a
    # transaction.
    #
    def rollback()
        if @db.transaction_active?
            begin 
                @db.rollback 
                @db.transaction
            rescue Exception => e
                raise DBI::Warning, "Statements were not closed prior to rollback"
            end
        else
            raise DBI::ProgrammingError.new("No active transaction.")
        end
    end

    def tables()
        ret = []
        result = @db.execute(%q(
            SELECT name FROM sqlite_master WHERE type IN ('table', 'view') 
            UNION ALL 
            SELECT name FROM sqlite_temp_master WHERE type in ('table', 'view') ORDER BY 1
        ))
        result.each{|row| ret.push(row[0])}
        ret
    end

    #
    # See DBI::BaseDatabase#columns.
    #
    # Additional Attributes:
    #
    # * sql_type: XOPEN integer SQL Type.
    # * nullable: true if NULL is allowed in this column.
    # * default: the value that will be used in new rows if this column
    #   receives no data.
    #
    def columns(table)
        @db.type_translation = false
        ret =
            @db.table_info(table).map do |hash|
                m = DBI::DBD::SQLite3.parse_type(hash['type'])
                h = { 
                    'name' => hash['name'],
                    'type_name' => m[1],
                    'sql_type' => 
                        begin
                            DBI.const_get('SQL_'+hash['type'].upcase)
                        rescue NameError
                            DBI::SQL_OTHER
                        end,
                    'nullable' => (hash['notnull'] == '0'),
                    'default' => (@attr['type_translation'] && (not hash['dflt_value'])) ? 
                                    @db.translator.translate(hash['type'], hash['dflt_value']) :
                                    hash['dflt_value'] 
                }

                h['precision'] = m[3].to_i if m[3]
                h['scale']     = m[5].to_i if m[5]

                h
            end
        @db.type_translation = @attr['type_translation']
        ret
    end

    def quote(value)
        ::SQLite3::Database.quote(value.to_s)
    end

    #
    # This method is used to aid the constructor and probably should not be
    # used independently.
    #
    def __generate_attr__()
        tt = @db.type_translation
        @db.type_translation = false
        [ 'auto_vacuum', 'cache_size', 'default_cache_size',
        'default_synchronous', 'default_temp_store', 'full_column_names',
        'synchronous', 'temp_store', 'type_translation' ].each do |key|
            unless @attr.has_key?(key) then
                @attr[key] = @db.__send__(key)
            end
        end
        @db.type_translation = tt
    end

    #
    # See #new for valid attributes.
    #
    # If Autocommit is set to true, commit happens immediately if a transaction
    # is open.
    #
    def []=(attr, value)
        case attr
        when 'AutoCommit'
            if value
                @db.commit if @db.transaction_active?
            else
                @db.transaction unless @db.transaction_active?
            end
        @attr[attr] = value
        when 'auto_vacuum', 'cache_size', 'count_changes',
          'default_cache_size', 'encoding', 'full_column_names',
          'page_size', 'short_column_names', 'synchronous',
          'temp_store', 'temp_store_directory'
          @db.__send__((attr+'='), value)
            @attr[attr] = @db.__send__(attr)
        when 'busy_timeout'
            @db.busy_timeout(value)
            @attr[attr] = value
        when 'busy_handler'
            @db.busy_timeout(&value)
            @attr[attr] = value
        when 'type_translation'
            @db.type_translation = value
            @attr[attr] = value
        else
            raise DBI::NotSupportedError
        end

        return value
    end
end
