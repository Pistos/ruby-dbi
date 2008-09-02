class ColumnInfo < Hash

   # Creates and returns a ColumnInfo object.  This represents metadata for
   # columns within a given table, such as the data type, whether or not the
   # the column is a primary key, etc.
   #
   # ColumnInfo is a subclass of Hash.
   #
   def initialize(hash=nil)
      self.update(hash) if hash
   end
    
    def self_value( key )
      v = self[ key.to_sym ]
      if v.nil?
        self[ key.to_s ]
      end
    end
   
   # Returns the column's name.
   def name
      self[ :name ] || self['name']
   end
   
   # Sets the column's name.
   def name=(val)
      self['name'] = val
   end
   
   # Returns a portable integer representation of the column's type.  Here are
   # the constant names (under DBI) and their respective values:
   #
   # SQL_CHAR      = 1
   # SQL_NUMERIC   = 2
   # SQL_DECIMAL   = 3
   # SQL_INTEGER   = 4
   # SQL_SMALLINT  = 5
   # SQL_FLOAT     = 6
   # SQL_REAL      = 7
   # SQL_DOUBLE    = 8
   # SQL_DATE      = 9 
   # SQL_TIME      = 10
   # SQL_TIMESTAMP = 11
   # SQL_VARCHAR   = 12
   #
   # SQL_LONGVARCHAR   = -1
   # SQL_BINARY        = -2
   # SQL_VARBINARY     = -3
   # SQL_LONGVARBINARY = -4
   # SQL_BIGINT        = -5
   # SQL_BIT           = -7
   # SQL_TINYINT       = -6
   #
   def sql_type
      self[ :sql_type ] || self['sql_type']
   end
   
   # Sets the integer representation for the column's type.
   def sql_type=(val)
      self['sql_type'] = val
   end
   
   # A string representation of the column's type, e.g. 'date'.
   def type_name
      self[ :type_name ] || self['type_name']
   end
   
   # Sets the representation for the column's type.
   def type_name=(val)
      self['type_name'] = val
   end
   
   # Returns the precision, i.e. number of bytes or digits.
   def precision
      self[ :precision ] || self['precision']
   end
   
   # Sets the precision, i.e. number of bytes or digits.
   def precision=(val)
      self['precision'] = val
   end
   
   # Returns the number of digits from right.
   def scale
      self[ :scale ] || self['scale']
   end
   
   # Sets the number of digits from right.
   def scale=(val)
      self['scale'] = val
   end
   
   # Returns the default value for the column, or nil if not set.
   def default_value( arg = nil )
      self_value 'default_value'
   end
   
   # Sets the default value for the column.
   def default_value=(val)
      self['default_value'] = val
   end
   
   # Returns whether or not the column is may contain a NULL.
   def nullable
      self_value 'nullable'
   end
   
   # Sets whether or not the column may contain a NULL.
   def nullable=(val)
      self['nullable'] = val
   end
   
   # Returns whether or not the column is indexed.
   def indexed
      self_value 'indexed'
   end
   
   # Sets whether or not the column is indexed.
   def indexed=(val)
      self['indexed'] = 'val'
   end
   
   # Returns whether or not the column is a primary key.
   def primary
      self_value 'primary'
   end
   
   # Sets whether or not the column is a primary key.
   def primary=(val)
      self['primary'] = val
   end
   
   # Returns whether or not data in the column must be unique.
   def unique
      self_value 'unique'
   end
   
   # Sets whether or not data in the column must be unique.
   def unique=(val)
      self['unique'] = val
   end

   # Aliases
   alias nullable? nullable
   alias is_nullable? nullable
   alias can_be_null? nullable

   alias indexed? indexed
   alias is_indexed? indexed

   alias primary? primary
   alias is_primary? primary

   alias unique? unique
   alias is_unique unique

   alias size precision
   alias size= precision=
   alias length precision
   alias length= precision=

   alias decimal_digits scale
   alias decimal_digits= scale=
end

require 'deprecated'

module DBI
    # This represents metadata for columns within a given table, such as the
    # data type, whether or not the the column is a primary key, etc.
    #
    # ColumnInfo is a delegate of Hash, but represents its keys indifferently,
    # coercing all strings to symbols. It also has ostruct-like features, f.e.:
    #
    #   h = ColumnInfo.new({ "foo" => "bar" })
    #   h[:foo] => "bar"
    #   h["foo"] => "bar"
    #   h.foo => "bar"
    #
    # All of these forms have assignment forms as well.
    #
    class ColumnInfo < DelegateClass(Hash)

        # Create a new ColumnInfo object.
        #
        # If no Hash is provided, one will be created for you. The hash will be
        # shallow cloned for storage inside the object, and an attempt will be
        # made to convert all string keys to symbols.
        #
        # In the event that both string and symbol keys are provided in the
        # initial hash, we cannot safely route around collisions and therefore
        # a TypeError is raised.
        #
        def initialize(hash=nil)
            @hash = hash.dup rescue nil
            @hash ||= Hash.new

            # coerce all strings to symbols
            @hash.each_key do |x|
                if x.kind_of? String
                    sym = x.to_sym
                    if @hash.has_key? sym
                        raise ::TypeError, 
                            "#{self.class.name} may construct from a hash keyed with strings or symbols, but not both" 
                    end
                    @hash[sym] = @hash[x]
                    @hash.delete(x)
                end
            end

            super(@hash)
        end

        def [](key)
            @hash[key.to_sym]
        end

        def []=(key, value)
            @hash[key.to_sym] = value
        end

        def default() # :nodoc; XXX hack to get around Hash#default
            method_missing(:default)
        end

        def method_missing(sym, value=nil)
            if sym.to_s =~ /=$/
                sym = sym.to_s.sub(/=$/, '').to_sym
                @hash[sym] = value
            elsif sym.to_s =~ /\?$/
                sym = sym.to_s.sub(/\?$/, '').to_sym
                @hash[sym]
            else
                @hash[sym]
            end
        end

        # Aliases - XXX soon to be deprecated
        def self.deprecated_alias(target, source) # :nodoc:
            define_method(target) { |*args| method_missing(source, *args) }
            deprecate target 
        end

        deprecated_alias :is_nullable?, :nullable
        deprecated_alias :can_be_null?, :nullable

        deprecated_alias :is_indexed?, :indexed

        deprecated_alias :is_primary?, :primary

        deprecated_alias :is_unique, :unique

        deprecated_alias :size, :precision
        deprecated_alias :size=, :precision=
        deprecated_alias :length, :precision
        deprecated_alias :length=, :precision=

        deprecated_alias :decimal_digits, :scale
        deprecated_alias :decimal_digits=, :scale=

        deprecated_alias :default_value, :default
        deprecated_alias :default_value=, :default=
    end
end
