require 'delegate'
require 'rubygems'
begin
    gem 'deprecated'
rescue LoadError => e
end

require 'deprecated'

module DBI
    class ColumnInfo < DelegateClass(Hash)
        # Creates and returns a ColumnInfo object.  This represents metadata for
        # columns within a given table, such as the data type, whether or not the
        # the column is a primary key, etc.
        #
        # ColumnInfo is a subclass of Hash.
        #
        def initialize(hash=nil)
            @hash = hash
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

        def default # :nodoc; XXX hack to get around Hash#default
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
        def self.deprecated_alias(target, source)
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
