#
# $Id: utils.rb,v 1.5 2006/01/29 06:14:19 djberg96 Exp $
#

module DBI
    module Utils
        def self.measure
            start = ::Time.now
            yield
            ::Time.now - start
        end

        ##
        # parse a string of the form "database=xxx;key=val;..."
        # or database:host and return hash of key/value pairs
        #
        # improved by John Gorman <jgorman@webbysoft.com>
        def self.parse_params(str)
            params = str.split(";")
            hash = {}
            params.each do |param| 
                key, val = param.split("=") 
                hash[key] = val if key and val
            end 
            if hash.empty?
                database, host = str.split(":")
                hash['database'] = database if database
                hash['host']     = host if host   
            end
            hash 
        end
    end # module Utils
end # module DBI

# FIXME this really needs to go into DBI::TypeUtil or similar
module DBI::Utils::ConvParam
    def self.conv_param(driver_name, *params)
        params.collect { |param| DBI::TypeUtil.convert(driver_name, param) }
    end
end

require 'dbi/utils/date'
require 'dbi/utils/time'
require 'dbi/utils/timestamp'
require 'dbi/utils/xmlformatter'
require 'dbi/utils/tableformatter'
