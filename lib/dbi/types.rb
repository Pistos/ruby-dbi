require 'time'
require 'bigdecimal'

module DBI
    #
    # Interface to convert SQL result sets to native Ruby types.
    #
    # Type is used to convert result sets, which differ from bound variables
    # (which generally go in the opposite direction). For those, see
    # DBI::TypeUtil#convert and DBI::TypeUtil#register_conversion.
    #
    # Type objects have a simple interface: they implement a +parse+ method
    # which takes the result from the DBD and attempts to convert it to the
    # native type. In the event that they do not do this successfully, they are
    # expected to return the object in its original form.
    #
    # As a result, many of the built-in Type classes fallback to simpler forms:
    # Float falls back to Integer, Integer to Varchar, etc. It's questionable
    # at this point if it's desirable to do this, but testing has so far proven
    # it a non-issue.
    #
    # To reiterate, it is *never acceptable* to return +nil+ or some other
    # placeholder when an object will not successfully parse. Return the object
    # handed to you.
    #
    # Types must also handle +nil+ as a result to parse. In this case, the
    # advisable solution is to just let the +nil+ pass through, as it's usually
    # indicative of a SQL NULL result.
    #
    # DBI::Row handles delegation of these objects as a converter for the
    # results. Typically, the type object is a class inferred from
    # DBI::TypeUtil#type_name_to_module ran against the ColumnInfo field
    # +type_name+. However, the the +dbi_type+ field can be used in its place
    # to directly associate a Type object with the column in the DBD, and
    # end-users can leverage StatementHandle#bind_coltype to manually tweak
    # this transformation.
    #
    # As stated before, Type objects are objects. These objects may be Modules
    # or Classes (and typically are), but there is no reason a traditional
    # constructed object with a +parse+ method cannot be returned; in fact, it
    # is used in a few spots to emulate complex types such as PostgreSQL
    # arrays. Look into the +dbi_type+ ColumnInfo field to pass these types
    # around.
    #
    module Type
        #
        # Represents a SQL NULL.
        #
        class Null
            def self.parse(obj)
                return nil if obj.to_s.match(/^null$/i)
                return obj
            end
        end

        #
        # Represents a SQL char or varchar. General fallback class.
        #
        class Varchar 
            def self.parse(obj)
                return obj unless obj
                return obj.to_s if obj.respond_to? :to_s
                return obj.to_str if obj.respond_to? :to_str
                return obj
            end
        end

        #
        # Represents a whole number. Falls back to Varchar.
        #
        class Integer < Varchar
            def self.parse(obj)
                return nil if Null.parse(obj).nil?
                return obj.to_i if obj.respond_to? :to_i
                super 
            end
        end

        #
        # Represents a decimal number with floating-point precision. Falls back
        # to Integer.
        #
        class Float < Integer
            def self.parse(obj)
                return nil if Null.parse(obj).nil?
                return obj.to_f if obj.respond_to? :to_f
                super
            end
        end

        #
        # Represents a Decimal with real precision (BigDecimal). Falls back to
        # Float.
        #
        class Decimal < Float
            def self.parse(obj)
                BigDecimal.new(obj) rescue super
            end
        end

        #
        # Represents a SQL TIMESTAMP and returns DateTime. Falls back to Null.
        #
        class Timestamp < Null
            def self.create(year, month, day, hour, min, sec, usec=0, of=0)
                # DateTime will remove leap and leap-leap seconds
                sec = 59 if sec > 59
                # store this before we modify it
                civil = year, month, day
                time  = hour, min, sec, usec
                if month <= 2
                    month += 12
                    year  -= 1
                end
                y   = year + 4800
                m   = month - 3
                jd  = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
                #fr  = hour / 24.0 + min / 1440.0 + sec / 86400.0
                # ridiculously, this line does the same thing but twice as fast... :/
                fr  = ::Time.gm(1970, 1, 1, hour, min, sec, usec).to_f / 86400
                date = ::DateTime.new!(jd + fr - 0.5, of, ::DateTime::ITALY)
                prefill_cache date, civil, time
                date
            end

            if RUBY_VERSION =~ /^1\.8\./
                def self.prefill_cache date, civil, time
                		time[3] /= 86400000000.0
                    date.instance_variable_set :"@__#{:civil.to_i}__", [civil]
                    date.instance_variable_set :"@__#{:time.to_i}__",  [time]
                end
            else
                def self.prefill_cache date, civil, time
                		time[3] /= 1000000.0
                    date.instance_variable_get(:@__ca__)[:civil.object_id] = civil
                    date.instance_variable_get(:@__ca__)[:time.object_id] = time
                end
            end

            def self.parse_string str
                # special casing the common formats here gives roughly an
                # 8-fold speed boost over using Date._parse
                case str
                when /^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2})(\.\d+)?)?(?: ([+-]?\d{2}):?(\d{2}))?$/
                    parts = $~[1..-4].map { |s| s.to_i }
                    parts << $7.to_f * 1000000.0
                    parts << ($8 ? ($8.to_f * 60 + $9.to_i) / 1440 : 0)
                else
                    parts = ::Date._parse(str).values_at(:year, :mon, :mday, :hour, :min, :sec, :sec_fraction, :offset)
                    # some defaults
                    today = nil
                    8.times do |i|
                        next if parts[i]
                        today ||= ::Time.now.to_a.values_at(5, 4, 3) + [0, 0, 0, 0, 0]
                        parts[i] = today[i]
                    end
                    parts[6] *= 1000000.0
                    parts[7] /= 86400.0
                end
                parts
            end

            def self.parse(obj)
                case obj
                when ::DateTime
                    return obj
                when ::Date
                    return create(obj.year, obj.month, obj.day, 0, 0, 0)
                when ::Time
                    return create(obj.year, obj.month, obj.day, obj.hour, obj.min, obj.sec, obj.usec, obj.utc_offset / 86400.0)
                else
                    obj = super
                    return obj unless obj
                    return create(*parse_string(obj.to_s))   if obj.respond_to? :to_s
                    return create(*parse_string(obj.to_str)) if obj.respond_to? :to_str
                    return obj
                end
            end
        end

        #
        # Represents a SQL BOOLEAN. Returns true/false. Falls back to Null.
        #
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
