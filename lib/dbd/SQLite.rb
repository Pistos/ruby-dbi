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
                    @autocommit = false
                    @autocommit = true        if attr_hash["AutoCommit"]

                    # open the database
                    begin
                        @db = ::SQLite::Database.new(dbname)
                    rescue Exception => e
                        raise DBI::OperationalError, "Couldn't open database #{dbname}: #{e.message}"
                    end
                end

                def disconnect
                    @db.close if @db and !@db.closed?
                    @db = nil
                end

                def prepare(stmt)
                    return Statement.new(stmt, self)
                end

                def ping
                    return !@db.closed?
                end

                def do(stmt, *bindvars)

                    # FIXME this *should* be building a statement handle and doing it that way.

                    # call self.bind with the statement and bindvars to produce sql to send to the driver
                    # XXX is there a binding API we can use instead?
                    # run the check_sql routine to ensure there are no nulls 
                    # send it to the database
                    # if error, throw DBI::DatabaseError
                end

                def tables
                    # select name from sqlite_master where type='table';
                    # XXX does sqlite use views too? not sure, but they need to be included according to spec
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

                def columns(tablename)
                    # execute PRAGMA table_info(tablename)
                    # fill out the name, type_name, nullable, and default entries in an hash which is a part of array 
                    # XXX it'd be nice if the spec was changed to do this k/v with the name as the key.
                end
            end

            class Statement < DBI::BaseStatement
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

                def initialize(stmt, dbh)
                    @dbh       = dbh
                    @statement = stmt
                    @attr      = { }
                    @params    = [ ]
                    @col_info  = nil
                    @rows      = [ ]
                end

                def bind_param(param, value, attributes)
                    # if param is a fixnum
                        # set the value to the index in @params that param specifies.
                    # else raise a DBI::InterfaceError with "Only ? parameters supported"

                    # XXX I assume this means that named bound parameters do not work
                end

                def execute(*params)
                    # do what Database#do does (which should be moved here and #do calls this instead)
                    # cache the column information for the selected columns (see Database#columns)
                    # if full_column_names is not set, run this code:
=begin
                            col_name_occurences = Hash.new(0)                    
                                                                                 
                            @col_info.each do |n|                                
                              col_name_occurences[n['name']] += 1                
                            end                                                  
                                                                                 
                            col_name_occurences.each do |name, anz|              
                              if anz > 1 then                                    
                                @col_info.each do |c|                            
                                  c['name'] = c['full_name'] if c['name'] == name
                                end                                              
                              end                                                
                            end                                                  
=end

                    # XXX yes, that's my way of saying, "I have no fucking idea
                    #     what this does, but it's probably important"
                end
                
                def cancel
                    # free all in-memory data relating to the result of the query
                end

                def finish
                    # finish() differs from cancel in only that it resets the
                    # "rpc" (row processed count, verified) and "rows" (probably the row returned amount)
                end

                def fetch
                    # fetch each row 
                    # if we have a result, convert it using the TYPE_CONV_MAP
                    # stuff it into @rows. XXX I really think this is a bad idea. 
                end

                def fetch_scroll(direction, offset)
                    # XXX this method is so poorly implemented it's disgusting. Replace completely.
                end

                def column_info
                    # accessor for @col_info
                end

                def rows
                    # if rpc is not -1, return it as a Number
                end

                def quote(obj)
                    # special (read: stupid) handling for Timestamps
                    # otherwise call quote in the superclass
                end
            end
        end
    end
end
