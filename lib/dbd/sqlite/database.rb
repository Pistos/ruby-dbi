#
# See DBI::BaseDatabase.
#
class DBI::DBD::SQLite::Database < DBI::BaseDatabase
    attr_reader :db
    attr_reader :attr_hash
    attr_accessor :open_handles

    #
    # Constructor. Valid attributes:
    #
    # * AutoCommit: Commit after every statement execution.
    #
    def initialize(dbname, user, auth, attr_hash)
        # FIXME why isn't this crap being done in DBI?
        unless dbname.kind_of? String
            raise DBI::InterfaceError, "Database Name must be a string"
        end

        unless dbname.length > 0
            raise DBI::InterfaceError, "Database Name needs to be length > 0"
        end

        unless attr_hash.kind_of? Hash
            raise DBI::InterfaceError, "Attributes should be a hash"
        end

        # FIXME handle busy_timeout in SQLite driver
        # FIXME handle SQLite pragmas in SQLite driver
        @attr_hash = attr_hash
        @open_handles = 0

        self["AutoCommit"] = true if self["AutoCommit"].nil?

        # open the database
        begin
            @db = ::SQLite::Database.new(dbname)
        rescue Exception => e
            raise DBI::OperationalError, "Couldn't open database #{dbname}: #{e.message}"
        end
    end

    def disconnect
        rollback rescue nil
        @db.close if @db and !@db.closed?
        @db = nil
    end

    def database_name
        st = DBI::DBD::SQLite::Statement.new('PRAGMA database_list', self)
        st.execute
        row = st.fetch
        st.finish

        return row[2]
    end

    def prepare(stmt)
        return DBI::DBD::SQLite::Statement.new(stmt, self)
    end

    def ping
        return !@db.closed?
    end

    def tables
        sth = prepare("select name from sqlite_master where type in ('table', 'view')")
        sth.execute
        tables = sth.fetch_all.flatten
        sth.finish
        return tables
        # FIXME does sqlite use views too? not sure, but they need to be included according to spec
    end

    def commit
        @db.commit if @db.transaction_active?
    end

    #
    # Rollback the transaction. SQLite has some issues with open statement
    # handles when this happens. If there are still open handles, a
    # DBI::Warning exception will be raised.
    #
    def rollback
        if @open_handles > 0
            raise DBI::Warning, "Leaving unfinished select statement handles while rolling back a transaction can corrupt your database or crash your program"
        end

        @db.rollback if @db.transaction_active?
    end

    def [](key)
        return @attr_hash[key]
    end

    #
    # See DBI::BaseDatabase#[]=.
    #
    # If AutoCommit is set to +true+ using this method, was previously +false+,
    # and we are currently in a transaction, The act of setting this will cause
    # an immediate commit.
    #
    def []=(key, value)

        old_value = @attr_hash[key]

        @attr_hash[key] = value

        # special handling of settings
        case key
        when "AutoCommit"
            # if the value being set is true and the previous value is false,
            # commit the current transaction (if any)
            # FIXME I still think this is a horrible way of handling this.
            if value and !old_value
                begin 
                    @dbh.commit
                rescue Exception => e
                end
            end
        end

        return @attr_hash[key]
    end

    def columns(tablename)
        return nil unless tablename and tablename.kind_of? String

        sth = prepare("PRAGMA table_info(?)")
        sth.bind_param(1, tablename)
        sth.execute
        columns = [ ]
        while row = sth.fetch
            column = { }
            column["name"] = row[1]

            m = DBI::DBD::SQLite.parse_type(row[2])
            column["type_name"] = m[1]
            column["precision"] = m[3].to_i if m[3]
            column["scale"]     = m[5].to_i if m[5]

            column["nullable"]  = row[3].to_i == 0
            column["default"]   = row[4]
            columns.push column
        end

        sth.finish
        return columns
        # XXX it'd be nice if the spec was changed to do this k/v with the name as the key.
    end
end
