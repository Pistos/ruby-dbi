require 'time'

module DBI
    class TypeUtil
        @@conversions = { }

        def self.register_conversion(driver_name, &block)
            raise "Must provide a block" unless block_given?
            @@conversions[driver_name] = block
        end

        def self.convert(driver_name, obj)
            newobj = obj
            if @@conversions[driver_name]
                newobj = @@conversions[driver_name].call(obj)
            end
            if newobj.object_id == obj.object_id
                return @@conversions["default"].call(newobj)
            end

            return newobj
        end

        def self.type_name_to_module(type_name)
            case type_name
            when /^int(?:\d+|eger)?$/i
                DBI::Type::Integer
            when /^varchar$/i, /^character varying$/i
                DBI::Type::Varchar
            when /^(?:float|real)$/i
                DBI::Type::Float
            when /^bool(?:ean)?$/i, /^tinyint$/i
                DBI::Type::Boolean
            when /^time(?:stamp(?:tz)?)?$/i
                DBI::Type::Timestamp
            else
                DBI::Type::Varchar
            end
        end
    end

    DBI::TypeUtil.register_conversion("default") do |obj|
          case obj
          when DBI::Binary # these need to be handled specially by the driver
              obj
          when ::NilClass
              "'NULL'"
          when ::TrueClass
              "'1'"
          when ::FalseClass
              "'0'"
          when ::Time, ::Date, ::DateTime
              "'#{::DateTime.parse(obj.to_s).strftime("%m/%d/%Y %H:%M:%S")}'"
          when ::String
              obj = obj.gsub(/'/) { "''" }
              "'#{obj}'"
          when ::Numeric
              obj.to_s
          else
              "'#{obj.to_s}'"
          end
    end

    module Type
        class Null
            def self.parse(obj)
                return nil if obj.to_s.match(/^null$/i)
                return obj
            end
        end

        class Varchar < Null
            def self.parse(obj)
                obj = super
                return obj unless obj
                return obj.to_s if obj.respond_to? :to_s
                return obj.to_str if obj.respond_to? :to_str
                return obj
            end
        end

        class Integer < Varchar
            def self.parse(obj)
                return obj.to_i if obj.respond_to? :to_i
                super 
            end
        end

        class Float < Integer
            def self.parse(obj)
                return obj.to_f if obj.respond_to? :to_f
                super 
            end
        end

        class Timestamp < Null
            def self.parse(obj)
                obj = super
                return obj unless obj
                case obj.class
                when ::DateTime
                    return obj
                when ::Date
                    return ::DateTime.parse(obj.to_s)
                when ::Time
                    return ::DateTime.parse(obj.to_s)
                else
                    return ::DateTime.parse(obj.to_s)   if obj.respond_to? :to_s
                    return ::DateTime.parse(obj.to_str) if obj.respond_to? :to_str
                    return obj
                end
            end
        end

        class Boolean
            def self.parse(obj)
                if !obj
                    return false
                elsif obj.kind_of? TrueClass
                    return true
                else
                    case obj
                    when 't'
                        return true
                    when 'f'
                        return false
                    end

                    if obj.respond_to? :to_i
                        if obj.to_i == 0
                            return false
                        else
                            return true
                        end
                    else
                        # punt
                        return nil
                    end
                end
            end
        end
    end
end
