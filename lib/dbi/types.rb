require 'time'

module DBI
    class TypeUtil
        @@conversions = { }

        def self.register_conversion(driver_name, &block)
            raise "Must provide a block" unless block_given?
            @@conversions[driver_name] = block
        end

        def self.convert(driver_name, obj)
            if @@conversions[driver_name]
                obj = @@conversions[driver_name].call(obj)
            end
            return @@conversions["default"].call(obj)
        end
    end

    DBI::TypeUtil.register_conversion("default") do |obj|
          case obj
          when NilClass
              'NULL'
          when TrueClass
              '1'
          when FalseClass
              '0'
          else
              obj.to_s
          end
    end

    module Type
        class Varchar < ::DBI::TypeUtil
            def self.parse(obj)
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

        class Timestamp 
            def self.parse(obj)
                case obj.class
                when DateTime
                    return obj.to_time
                when Date
                    return Time.parse(obj.to_s)
                when Time
                    return obj
                else
                    return Time.parse(obj.to_s)   if obj.respond_to? :to_s
                    return Time.parse(obj.to_str) if obj.respond_to? :to_str
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
