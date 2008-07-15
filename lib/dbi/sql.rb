#
# $Id: sql.rb,v 1.3 2006/03/27 20:25:02 francis Exp $
#
# parts extracted from Jim Weirichs DBD::Pg
#

module DBI
require "dbi/utils"
require "parsedate"
require "time"

module SQL

  ## Is the SQL statement a query?
  def SQL.query?(sql)
    sql =~ /^\s*select\b/i
  end


  ####################################################################
  # Mixin module useful for expanding SQL statements.
  #
  # FIXME gut this
  module BasicQuote
  end # module BasicQuote

  ####################################################################
  # Mixin module useful for binding arguments to an SQL string.
  #
  module BasicBind

    ## Break the sql string into parts.
    #
    # This is NOT a full lexer for SQL.  It just breaks up the SQL
    # string enough so that question marks, double question marks and
    # quoted strings are separated.  This is used when binding
    # arguments to "?" in the SQL string.  
    #
    # C-style (/* */) and Ada-style (--) comments are handled.
    # Note: Nested C-style comments are NOT handled!
    #
    def tokens(sql)
      sql.scan(%r{
        (
            -- .*                               (?# matches "--" style comments to the end of line or string )
        |   -                                   (?# matches single "-" )
        |
            /[*] .*? [*]/                       (?# matches C-style comments )
        |   /                                   (?# matches single slash )    
        |
            ' ( [^'\\]  |  ''  |  \\. )* '      (?# match strings surrounded by apostophes )
        |
            " ( [^"\\]  |  ""  |  \\. )* "      (?# match strings surrounded by " )
        |
            \?\??                               (?# match one or two question marks )
        |
            [^-/'"?]+                           (?# match all characters except ' " ? - and / )
            
        )}x).collect {|t| t.first}
    end

  end # module BasicBind


  class PreparedStatement
    include BasicBind # for method tokens(sql)

    attr_accessor :unbound

    def initialize(quoter, sql)
      @quoter, @sql = quoter, sql
      prepare
    end

    def bind(args)
      if @arg_index < args.size
        raise "Too many SQL parameters"
      elsif @arg_index > args.size
        raise "Not enough SQL parameters"
      end

      @unbound.each do |res_pos, arg_pos|
        @result[res_pos] = args[arg_pos]
      end

      @result.join("")
    end

    private

    def prepare
      @result = [] 
      @unbound = {}
      pos = 0
      @arg_index = 0

      tokens(@sql).each { |part|
        case part
        when '?'
              @result[pos] = nil
              @unbound[pos] = @arg_index
              pos += 1
              @arg_index += 1
        when '??'
              if @result[pos-1] != nil
                @result[pos-1] << "?"
              else
                @result[pos] = "?"
                pos += 1
              end
        else
              if @result[pos-1] != nil
                @result[pos-1] << part
              else
                @result[pos] = part
                pos += 1
              end
        end
      }
    end
  end

end # module SQL
end # module DBI
