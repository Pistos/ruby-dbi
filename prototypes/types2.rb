require 'date'
require 'time'

#
# General example, these would be types that would exist in DBI proper
#

module DBI
    class DBI::Type
        def self.parse(obj, type=nil, dbh=nil)
            if type 
                sym = ( "to_" + type.to_s ).to_sym
                begin
                    return self.__send__(sym, obj) 
                rescue ::NoMethodError 
                    self.to_type(obj)
                end
            else
                return self.to_type(obj)
            end
        end

        def self.coerce(obj, type=nil, dbh=nil)
            if type
                sym = ( "from_" + type.to_s ).to_sym
                begin
                    return self.__send__(sym, obj) 
                rescue ::NoMethodError 
                    self.from_type(obj)
                end
            else
                return self.from_type(obj)
            end
        end

        def self.from_type(obj)
            obj.to_s rescue obj.to_str rescue obj
        end

        def self.to_type(obj)
            obj
        end
    end

    class DBI::Type::Null < DBI::Type
        def self.to_type(obj)
            return obj unless obj
            return nil if obj.to_s.match(/^null$/i)
            return obj
        end

        def self.from_type(obj)
            obj
        end
    end

    class DBI::Type::Integer < DBI::Type::Null
        def self.parse(obj)
            obj = super
            return obj unless obj
            return obj.to_i if obj.respond_to? :to_i
            return obj
        end
    end

    class DBI::Type::Timestamp < DBI::Type::Null
        def self.to_type(obj)
            obj = super
            return obj unless obj

            case obj
            when ::DateTime
                return obj
            when ::Date
                return ::DateTime.strptime(obj.to_s, "%Y-%m-%d")
            when ::Time
                return ::DateTime.parse(obj.to_s)
            when ::Integer
                return ::DateTime.parse(::Time.at(obj).to_s)
            else
                return ::DateTime.parse(obj.to_s)   if obj.respond_to? :to_s
                return ::DateTime.parse(obj.to_str) if obj.respond_to? :to_str
                return obj
            end
        end

        def self.from_type(obj)
            obj = super
            return obj unless obj

            case obj
            when ::DateTime
                return obj.to_s # produces ISO8601
            when ::Time
                return obj.iso8601
            when ::Integer
                return ::Time.at(obj).iso8601
            else
                return obj
            end
        end
    end
end

module DBI::DBD
    class Pg

        #
        # during connect time, after DatabaseHandle initialization, the hash
        # that DatabaseHandle#type_map provides would be tweaked to take
        # advantage of the available date formats.
        #
        # See 'PgDatabaseHandle' below for a mock.
        #

        class Type
            class Timestamp < DBI::Type::Timestamp
                def self.from_dmy(obj)
                    return obj if DBI::Type::Null.parse(obj).nil?

                    case obj
                    when ::DateTime, ::Time
                        obj.strftime("%d/%m/%Y %H:%M:%S")
                    when ::Integer
                        ::Time.at(obj).strftime("%d/%m/%Y %H:%M:%S")
                    else
                        # punt... this will actually try the baseline
                        # conversion at this point
                        raise "Crap!"
                    end
                end

                def self.to_dmy(obj)
                    return obj if DBI::Type::Null.parse(obj).nil?

                    # realistically all there needs to be is a check for the
                    # type ruby-pg typically returns and string, but to be
                    # complete I'm showing how it could be done if the type was
                    # less clear.

                    case obj
                    when ::DateTime
                        return obj
                    when ::Time  
                        return ::DateTime.parse(obj.to_s)
                    else
                        return ::DateTime.strptime(obj, "%d/%m/%Y %H:%M:%S")
                    end
                end
            end
        end
    end
end 

#
# this is just used to emulate the methods a DatabaseHandle would have to
# faciliate this.. certainly not a full (or correct) mirroring of the DBI API.
#
class DatabaseHandle

    attr_accessor :columns
    
    def outbound_type_map
        {
            'timestamp' => [DBI::Type::Timestamp]
        }
    end

    def inbound_type_map
        {
            ::DateTime => [DBI::Type::Timestamp],
            ::Time     => [DBI::Type::Timestamp]
        }
    end

    # humor me while I completely break DBI for the sake of brevity..
    def execute(*bindvars)
        bindvars.collect do |var|
            type_info = inbound_type_map[var.class]
            type_info[0].coerce(var, type_info[1], self)
        end
    end

    def fetch(*bindvars)
        ret = []

        bindvars.each_with_index do |var, i|
            type_info = outbound_type_map[columns[i]]
            ret.push type_info[0].parse(var, type_info[1], self)
        end
        
        return ret
    end
end

class PgDatabaseHandle < DatabaseHandle
    def outbound_type_map
        {
            'timestamp' => [DBI::DBD::Pg::Type::Timestamp, :dmy]
        }
    end

    def inbound_type_map
        {
            ::DateTime => [DBI::DBD::Pg::Type::Timestamp, :dmy],
            ::Time     => [DBI::DBD::Pg::Type::Timestamp, :dmy]
        }
    end
end

# ok! now for the functional example:

if __FILE__ == $0

    dbh = DatabaseHandle.new
    dbh.columns = %w(timestamp timestamp)
    # this would go TO the database..
    p dbh.execute(DateTime.now, Time.now)
    # this would come FROM the database...
    p dbh.fetch(Time.now.iso8601, DateTime.now.to_s)

    # now the Pg example:
    dbh = PgDatabaseHandle.new
    dbh.columns = %w(timestamp timestamp)

    # this would go TO the database..
    p dbh.execute(DateTime.now, Time.now)
    # this would come FROM the database...
    p dbh.fetch(Time.now.strftime("%d/%m/%Y %H:%M:%S"), DateTime.now.strftime("%d/%m/%Y %H:%M:%S"))
    
    # this should fail appropriately
    begin
        dbh.fetch(Time.now.iso8601, DateTime.now.to_s)
    rescue Exception
        puts "this failed like it should"
    end
end
