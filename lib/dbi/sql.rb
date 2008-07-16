#
# $Id: sql.rb,v 1.3 2006/03/27 20:25:02 francis Exp $
#
# parts extracted from Jim Weirichs DBD::Pg
#

require "dbi/utils"
require "parsedate"
require "time"

module DBI
    module SQL
        ## Is the SQL statement a query?
        def self.query?(sql)
            sql =~ /^\s*select\b/i
        end
    end # module SQL
end # module DBI

require 'dbi/sql/preparedstatement'
