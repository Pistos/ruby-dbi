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
            def self.check_sql(sql)
                raise DBI::DatabaseError, "Bad SQL: SQL cannot contain nulls" if sql =~ /\0/
            end

            class Driver
                def initialize(*params)
                end

                def connect(*params)
                end
            end

            class Database
                include DBI::SQL::BasicBind

                def disconnect(*params)
                end

                def prepare(*params)
                end

                def ping(*params)
                end

                def do(*params)
                end

                def tables(*params)
                end

                def commit(*params)
                end

                def rollback(*params)
                end

                def [](*params)
                end

                def []=(*params)
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
