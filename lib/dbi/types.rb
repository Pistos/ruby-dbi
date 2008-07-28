require 'time'
require 'bigdecimal'

module DBI
    module Type
        class Null
            def self.parse(obj)
                return nil if obj.to_s.match(/^null$/i)
                return obj
            end
        end

        class Varchar 
            def self.parse(obj)
                return obj unless obj
                return obj.to_s if obj.respond_to? :to_s
                return obj.to_str if obj.respond_to? :to_str
                return obj
            end
        end

        class Integer < Varchar
            def self.parse(obj)
                return nil if Null.parse(obj).nil?
                return obj.to_i if obj.respond_to? :to_i
                super 
            end
        end

        class Float < Integer
            def self.parse(obj)
                return nil if Null.parse(obj).nil?
                return obj.to_f if obj.respond_to? :to_f
                super
            end
        end

        class Decimal < Float
            def self.parse(obj)
                BigDecimal.new(obj) rescue super
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

        class Boolean < Null
            def self.parse(obj)
                obj = super

                return nil if obj.nil?

                if obj == false or obj.kind_of? FalseClass
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
