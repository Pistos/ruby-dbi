#
# Type Management for PostgreSQL-specific types.
#
# See DBI::Type and DBI::TypeUtil for more information.
#
module DBI::DBD::Pg::Type
    #
    # ByteA is a special escaped form of binary data, suitable for inclusion in queries.
    #
    # This class is an attempt to abstract that type so you do not have to
    # concern yourself with the conversion issues.
    #
    class ByteA

        attr_reader :original
        attr_reader :escaped

        #
        # Build a new ByteA object.
        #
        # The data supplied is the unescaped binary data you wish to put in the
        # database.
        #
        def initialize(obj)
            @original = obj
            @escaped = escape_bytea(obj)
            @original.freeze
            @escaped.freeze
        end

        #
        # Escapes the supplied data. Has no effect on the object.
        #
        def escape_bytea(str)
            PGconn.escape_bytea(str)
        end

        #
        # Returns the original data.
        #
        def to_s
            return @original.dup
        end

        #
        # Class method to escape the data into ByteA format.
        #
        def self.escape_bytea(str)
            self.new(str).escaped
        end

        #
        # Class method to unescape the ByteA data and present it as a string.
        #
        def self.parse(obj)

            return nil if obj.nil?

            # FIXME there's a bug in the upstream 'pg' driver that does not
            # properly decode bytea, leaving in an extra slash for each decoded
            # character.
            #
            # Fix this for now, but beware that we'll have to unfix this as
            # soon as they fix their end.
            ret = PGconn.unescape_bytea(obj)

            # XXX 
            # String#split does not properly create a full array if the the
            # string ENDS in the split regex, unless this oddball -1 argument is supplied.
            #
            # Another way of saying this:
            # if foo = "foo\\\\\" and foo.split(/\\\\/), the result will be
            # ["foo"]. You can add as many delimiters to the end of the string
            # as you'd like - the result is no different.
            #

            ret = ret.split(/\\\\/, -1).collect { |x| x.length > 0 ? x.gsub(/\\[0-7]{3}/) { |y| y[1..3].oct.chr } : "" }.join("\\")
            ret.gsub!(/''/, "'")
            return ret
        end
    end

    #
    # PostgreSQL arrays are simply a specification that sits on top of normal
    # types. They have a specialized string grammar and this class facilitates
    # converting that syntax and the types within those arrays.
    #
    class Array

        attr_reader :base_type

        #
        # +base_type+ is a DBI::Type that is used to parse the inner types when
        # a non-array one is found.
        #
        # For instance, if you had an array of integer, one would pass
        # DBI::Type::Integer here.
        #
        def initialize(base_type)
            @base_type = base_type
        end

        #
        # Object method. Please note that this is different than most DBI::Type
        # classes! One must initialize an Array object with an appropriate
        # DBI::Type used to convert the indices of the array before this method
        # can be called.
        #
        # Returns an appropriately converted array.
        #
        def parse(obj)
            if obj.nil?
                nil
            elsif obj.index('{') == 0 and obj.rindex('}') == (obj.length - 1)
                convert_array(obj)
            else
                raise "Not an array"
            end
        end

        #
        # Parse a PostgreSQL-Array output and convert into ruby array. This
        # does the real parsing work.
        #
        def convert_array(str)

            array_nesting = 0         # nesting level of the array
            in_string = false         # currently inside a quoted string ?
            escaped = false           # if the character is escaped
            sbuffer = ''              # buffer for the current element
            result_array = ::Array.new  # the resulting Array

            str.each_byte { |char|    # parse character by character
                char = char.chr         # we need the Character, not it's Integer

                if escaped then         # if this character is escaped, just add it to the buffer
                    sbuffer += char
                    escaped = false
                    next
                end

                case char               # let's see what kind of character we have
                    #------------- {: beginning of an array ----#
                when '{'
                    if in_string then     # ignore inside a string
                        sbuffer += char
                        next
                    end

                if array_nesting >= 1 then  # if it's an nested array, defer for recursion
                    sbuffer += char
                end
                array_nesting += 1          # inside another array

                #------------- ": string deliminator --------#
                when '"'
                    in_string = !in_string      

                    #------------- \: escape character, next is regular character #
                when "\\"     # single \, must be extra escaped in Ruby
                    if array_nesting > 1
                        sbuffer += char
                    else
                        escaped = true
                    end

                    #------------- ,: element separator ---------#
                when ','
                    if in_string or array_nesting > 1 then  # don't care if inside string or
                        sbuffer += char                       # nested array
                    else
                        if !sbuffer.is_a? ::Array then
                            sbuffer = @base_type.parse(sbuffer)
                        end
                        result_array << sbuffer               # otherwise, here ends an element
                        sbuffer = ''
                    end

                #------------- }: End of Array --------------#
                when '}' 
                    if in_string then                # ignore if inside quoted string
                        sbuffer += char
                        next
                    end

                    array_nesting -=1                # decrease nesting level

                    if array_nesting == 1            # must be the end of a nested array 
                        sbuffer += char
                        sbuffer = convert_array( sbuffer )  # recurse, using the whole nested array
                    elsif array_nesting > 1          # inside nested array, keep it for later
                        sbuffer += char
                    else                             # array_nesting = 0, must be the last }
                        if !sbuffer.is_a? ::Array then
                            sbuffer = @base_type.parse( sbuffer )
                        end

                        result_array << sbuffer unless sbuffer.nil? # upto here was the last element
                    end

                    #------------- all other characters ---------#
                else
                    sbuffer += char                 # simply append
                end
            } 
            return result_array
        end # convert_array()
    end
end
