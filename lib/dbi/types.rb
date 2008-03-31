require 'time'

module DBI
    class TypeUtil
        @@conversions = { } # old perl habits die hard

        def self.register_conversion(klass, &block)
            raise "Not a class" unless klass.kind_of? Class
            raise "Must provide a block" unless block_given?
            @@conversions[klass] = block
        end

        def self.convert(sth, obj)
            package = Object.const_get(sth.name.split(/::/)[0..2].join("::"))
            return @@conversions[package].call(obj)
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
                    return Time.parse(obj.to_s) if obj.respond_to? :to_s
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
                    if obj.respond_to :to_i
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
