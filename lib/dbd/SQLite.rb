###############################################################################
#
# DBD::SQLite - a DBD for SQLite for versions < 3
#
# Uses Jamis Buck's 'sqlite-ruby' driver to interface with SQLite directly
#
# (c) 2008 Erik Hollensbe & Christopher Maujean.
#
################################################################################

begin
    require 'rubygems'
    gem 'sqlite'
rescue Exception => e
end

require 'sqlite'

module DBI
    module DBD
        class SQLite

            USED_DBD_VERSION = "0.1"

            # XXX I'm starting to think this is less of a problem with SQLite
            # and more with the old C DBD
            def self.check_sql(sql)
                raise DBI::DatabaseError, "Bad SQL: SQL cannot contain nulls" if sql =~ /\0/
            end

            class Driver < DBI::BaseDriver
                def initialize
                    super USED_DBD_VERSION
                end

                def connect(dbname, user, auth, attr_hash)
                    return Database.new(dbname, user, auth, attr_hash)
                end
            end

            class Database < DBI::BaseDatabase
                include DBI::SQL::BasicBind

                attr_reader :db
                attr_reader :attr_hash
                attr_accessor :open_handles

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

                def prepare(stmt)
                    return Statement.new(stmt, self)
                end

                def ping
                    return !@db.closed?
                end

                def tables
                    sth = prepare("select name from sqlite_master where type='table'")
                    sth.execute
                    tables = sth.fetch_all.flatten
                    sth.finish
                    return tables
                    # FIXME does sqlite use views too? not sure, but they need to be included according to spec
                end

                def commit
                    @db.commit if @db.transaction_active?
                end

                def rollback
                    if @open_handles > 0
                        raise DBI::Warning, "Leaving unfinished select statement handles while rolling back a transaction can corrupt your database or crash your program"
                    end

                    @db.rollback if @db.transaction_active?
                end

                def [](key)
                    return @attr_hash[key]
                end

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
                    # execute PRAGMA table_info(tablename)
                    # fill out the name, type_name, nullable, and default entries in an hash which is a part of array 
                    # XXX it'd be nice if the spec was changed to do this k/v with the name as the key.
                end
            end

            class Statement < DBI::BaseStatement
                include DBI::SQL::BasicBind
                include DBI::SQL::BasicQuote

                DBI_TYPE_MAP = [
                    [ /^INT(EGER)?$/i,          DBI::SQL_INTEGER ],
                    [ /^(OID|ROWID|_ROWID_)$/i, DBI::SQL_OTHER   ],
                    [ /^FLOAT$/i,               DBI::SQL_FLOAT   ],
                    [ /^REAL$/i,                DBI::SQL_REAL    ],
                    [ /^DOUBLE$/i,              DBI::SQL_DOUBLE  ],
                    [ /^DECIMAL/i,              DBI::SQL_DECIMAL ],
                    [ /^(BOOL|BOOLEAN)$/i,      DBI::SQL_BOOLEAN ], 
                    [ /^TIME$/i,                DBI::SQL_TIME    ],
                    [ /^DATE$/i,                DBI::SQL_DATE    ],
                    [ /^TIMESTAMP$/i,           DBI::SQL_TIMESTAMP ], 
                    [ /^(VARCHAR|TEXT)/i,       DBI::SQL_VARCHAR ],
                    [ /^CHAR$/i,                DBI::SQL_CHAR    ],
                ]

                def initialize(stmt, dbh)
                    @dbh       = dbh
                    @statement = DBI::SQL::PreparedStatement.new(@dbh, stmt)
                    @attr      = { }
                    @params    = [ ]
                    @rows      = [ ]
                    @result_set = nil
                    @dbh.open_handles += 1
                end

                def bind_param(param, value, attributes=nil)
                    unless param.kind_of? Fixnum
                        raise DBI::InterfaceError, "Only numeric parameters are supported"
                    end

                    @params[param-1] = value

                    # FIXME what to do with attributes? are they important in SQLite?
                end

                def execute
                    sql = @statement.bind(@params)
                    DBI::DBD::SQLite.check_sql(sql)
                   
                    begin
                        unless @dbh.db.transaction_active?
                            @dbh.db.transaction 
                        end
                        @result_set = @dbh.db.query(sql)
                        @dbh.commit if @dbh["AutoCommit"]
                    rescue Exception => e
                        raise DBI::DatabaseError, e.message
                    end
                end
               
                alias :finish :cancel

                def finish
                    # nil out the result set
                    @result_set.close if @result_set
                    @result_set = nil
                    @rows = nil
                    @dbh.open_handles -= 1
                end

                def fetch
                    return nil if @result_set.eof?
                    
                    row = @result_set.next
                    return nil unless row
                   
                    # convert types. FIXME this should *really* not be done in the driver

                    coerce  = DBI::SQL::BasicQuote::Coerce.new

                    columns = column_info
                    new_row = []

                    row.each_with_index do |col, i|
                        case columns[i]["sql_type"]
                        when SQL_BOOLEAN
                            col = coerce.as_bool(col)
                        when SQL_FLOAT, SQL_REAL, SQL_DOUBLE
                            col = coerce.as_float(col)
                        when SQL_INTEGER
                            col = coerce.as_int(col)
                        when SQL_TIME
                            col = coerce.as_time(col)
                        when SQL_TIMESTAMP
                            col = coerce.as_timestamp(col)
                        when SQL_DATE
                            col = coerce.as_date(col)
                        end

                        new_row.push col
                    end

                    # XXX this is needed for fetch_scroll
                    @rows.push new_row

                    return new_row
                end

                def fetch_scroll(direction, offset)
                    # XXX this method is so poorly implemented it's disgusting. Replace completely.
                end

                def column_info
                    columns = [ ]

                    # FIXME this shit should *really* be abstracted into DBI
                    # FIXME this still doesn't handle nullable/unique/default stuff.
                    @result_set.columns.each_with_index do |name, i|
                        columns[i] = { } unless columns[i]
                        columns[i]["name"] = name
                        type_name = @result_set.types[i]

                        m = type_name.match(/^([^\(]+)(\((\d+)(,(\d+))?\))?$/)
                        
                        columns[i]["type_name"] = m[1]
                        columns[i]["precision"] = m[3].to_i if m[3]
                        columns[i]["scale"]     = m[5].to_i if m[5]
                        DBI_TYPE_MAP.each do |map|
                            if columns[i]["type_name"] =~ map[0]
                                columns[i]["sql_type"] = map[1]
                                break
                            end
                        end
                    end
                   
                    return columns
                end

                def rows
                    return @dbh.db.changes
                end
            end
        end
    end
end
