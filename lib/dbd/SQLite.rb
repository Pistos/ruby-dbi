################################################################################
#
# DBD::SQLite - a DBD for SQLite for versions < 3
#
# Uses Jamis Buck's 'sqlite-ruby' driver to interface with SQLite directly
#
# (c) 2008 Erik Hollensbe & Christopher Maujean.
#
################################################################################

class DBI
    class DBD
        class SQLite

            USED_DBD_VERSION = "0.1"

            def self.check_sql(sql)
                raise DBI::DatabaseError, "Bad SQL: SQL cannot contain nulls" if sql =~ /\0/
            end

            class Driver
                def initialize
                    # this may be wrong - see line 95 in SQLite.c and ruby's
                    # README.EXT
                    super USED_DBD_VERSION
                end

                def connect(dbname, user, auth, attr_hash)
                    # dbname should be a string
                    # attr should be a hash
                    
                    # fill self with attributes according to the `sDatabase` struct in SQLite.c 
                    # turn autocommit and full_column_names off in the handle
                    # turn autocommit on if attr_hash has the key AutoCommit set
                    # if "sqlite_full_column_names" is set, turn full_column_names on in the handle
                 
                    # connect to sqlite (open the dbfile)
                    # if we fail, raise a DBI::OperationalError with the error message returned by the handle

                    # if autocommit is off, start a transaction
                    # if not ok, throw a DBI::DatabaseError with the error string.

                    # turn full_column_names on unconditionally (wtf? see above)
                end
            end

            class Database
                include DBI::SQL::BasicBind

                def disconnect
                    # disconnect the sqlite database
                    # return nil
                end

                def prepare(*params)
                end

                def ping
                    return true # FIXME we could check if the file exists and
                                # the db is opened... I think there's more we can do here.
                end

                def do(stmt, *bindvars)
                end

                def tables(*params)
                end

                def commit
                    # if autocommit is 0
                        # end the current transaction and start a new one.
                        # raise a DBI::DatabaseError if we fail it
                    # if autocommit is 1
                        # warn that commit is ineffective while AutoCommit is on.

                    # return nil
                end

                def rollback
                    # if autocommit is 0
                        # rollback the current transaction and start a new one
                        # raise a DBI::DatabaseError if we fail it
                    # if autocommit is 1
                        # warn that rollback is ineffective while AutoCommit is on

                    # return nil
                end

                def [](key)
                    # check the key to ensure it's a string

                    # if the key is non-nil:
                        # if requested, coerce the autocommit value to true/false FIXME not sure if this is the best idea
                        # if requested, coerce sqlite_full_column_names to t/f FIXME not even sure if this is necessary.
                    # else return nil

                    # XXX this whole routine might be pointless.
                end

                def []=(key, value)
                    # check the key to ensure it's a string
                    
                    # if our key is AutoCommit
                    # and our value is true
                        # turn AutoCommit on
                        # immediately commit the transaction XXX I think this is a *horrible* handling of this. 
                        # raise a DBI::DatabaseError if this fails
                    # else, if our value is false 
                        # start a transaction
                        # raise a DBI::DatabaseError if this fails 

                    # if our key is "sqlite_full_column_names"
                    # FIXME jesus, this does nothing but toggle the value... I still can't find a place where this actually affects the library.
                end

                def columns(*params)
                end
            end

            class Statement 
                include DBI::SQL::BasicBind
                include DBI::SQL::BasicQuote

                #
                # NOTE these two constants are taken directly out of the old
                #      SQLite.c. Not sure of its utility yet.
                #

                TYPE_CONV_MAP = 
                    [                                                                     
                        [ /^INT(EGER)?$/i,            proc {|str, c| c.as_int(str) } ],     
                        [ /^(OID|ROWID|_ROWID_)$/i,   proc {|str, c| c.as_int(str) }],      
                        [ /^(FLOAT|REAL|DOUBLE)$/i,   proc {|str, c| c.as_float(str) }],    
                        [ /^DECIMAL/i,                proc {|str, c| c.as_float(str) }],    
                        [ /^(BOOL|BOOLEAN)$/i,        proc {|str, c| c.as_bool(str) }],     
                        [ /^TIME$/i,                  proc {|str, c| c.as_time(str) }],     
                        [ /^DATE$/i,                  proc {|str, c| c.as_date(str) }],     
                        [ /^TIMESTAMP$/i,             proc {|str, c| c.as_timestamp(str) }] 
                        # [ /^(VARCHAR|CHAR|TEXT)/i,    proc {|str, c| c.as_str(str).dup } ]  
                    ]                                                                     

                CONVERTER = DBI::SQL::BasicQuote::Coerce.new

                # FIXME this definitely needs to be a private method
                CONVERTER_PROC = proc do |tm, cv, val, typ|
                    ret = val.dup             
                    tm.each do |reg, pr|      
                        if typ =~ reg           
                            ret = pr.call(val, cv)
                            break                 
                        end                     
                    end                       
                    ret                       
                end

                def bind_param(*params)
                end

                def execute(*params)
                end

                def finish(*params)
                end

                def cancel(*params)
                end

                def fetch(*params)
                end

                def fetch_scroll(*params)
                end

                def column_info(*params)
                end

                def rows(*params)
                end

                def quote(*params)
                end
            end
        end
    end
end
