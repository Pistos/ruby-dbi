$LOAD_PATH.unshift(File.dirname(__FILE__))
#
# Ruby/DBI
#
# Copyright (c) 2001, 2002, 2003 Michael Neumann <mneumann@ntecs.de>
# Copyright (c) 2008 Erik Hollensbe <erik@hollensbe.org>
# 
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions 
# are met:
# 1. Redistributions of source code must retain the above copyright 
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in the 
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
# THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# $Id: dbi.rb,v 1.8 2006/09/03 04:05:29 pdubois Exp $
#

require "find"
require "dbi/row"
require "dbi/utils"
require "dbi/sql"
require "dbi/columninfo"
require 'dbi/types'
require 'dbi/sql_type_constants'
require 'dbi/exceptions'
require 'dbi/binary'
require 'dbi/handles'
require "date"
require "thread"
require 'monitor'

module DBI
   VERSION = "0.2.0"
   
   module DBD
      API_VERSION = "0.3"
   end
   
   #  Module functions (of DBI)
   DEFAULT_TRACE_MODE = 2
   DEFAULT_TRACE_OUTPUT = STDERR
   
   # TODO: Is using class variables within a module such a wise idea? - Dan B.
   @@driver_map     = Hash.new
   @@driver_monitor = ::Monitor.new()
   @@trace_mode     = DEFAULT_TRACE_MODE
   @@trace_output   = DEFAULT_TRACE_OUTPUT
   @@caseless_driver_name_map = nil
   
   class << self
      
      # Establish a database connection.  This is mostly a facade for the
      # DBD's connect method.
      def connect(driver_url, user=nil, auth=nil, params=nil, &p)
         dr, db_args = _get_full_driver(driver_url)
         dh = dr[0] # driver-handle
         dh.connect(db_args, user, auth, params, &p)
      end
      
      # Load a DBD and returns the DriverHandle object
      def get_driver(driver_url)
         _get_full_driver(driver_url)[0][0]  # return DriverHandle
      end
      
      # Extracts the db_args from driver_url and returns the correspondeing
      # entry of the @@driver_map.
      def _get_full_driver(driver_url)
         db_driver, db_args = parse_url(driver_url)
         db_driver = load_driver(db_driver)
         dr = @@driver_map[db_driver]
         [dr, db_args]
      end
      
      def trace(mode=nil, output=nil)
         @@trace_mode   = mode   || @@trace_mode   || DBI::DEFAULT_TRACE_MODE
         @@trace_output = output || @@trace_output || DBI::DEFAULT_TRACE_OUTPUT
      end
     
      def collect_drivers
         drivers = { }
         # FIXME rewrite this to leverage require and be more intelligent
         path = File.join(File.dirname(__FILE__), "dbd", "*.rb")
         Dir[path].each do |f|
            if File.file?(f)
               driver = File.basename(f, ".rb")
               drivers[driver] = f
            end
         end

         return drivers
      end

      # Returns a list of the currently available drivers on your system in
      # 'dbi:driver:' format.
      def available_drivers
          drivers = []
          collect_drivers.each do |key, value|
              drivers.push("dbi:#{key}:")
          end

          return drivers
      end
      
      def data_sources(driver)
         db_driver, = parse_url(driver)
         db_driver = load_driver(db_driver)
         dh = @@driver_map[db_driver][0]
         dh.data_sources
      end
      
      def disconnect_all( driver = nil )
         if driver.nil?
            @@driver_map.each {|k,v| v[0].disconnect_all}
         else
            db_driver, = parse_url(driver)
            @@driver_map[db_driver][0].disconnect_all
         end
      end
      
      private
     
      def load_driver(driver_name)
          @@driver_monitor.synchronize do
              unless @@driver_map[driver_name]
                  dc = driver_name.downcase
                  
                  # caseless look for drivers already loaded
                  found = @@driver_map.keys.find {|key| key.downcase == dc}
                  return found if found

                  begin
                      require "dbd/#{driver_name}"
                  rescue LoadError => e1
                      # see if you can find it in the path
                      unless @@caseless_driver_name_map
                          @@caseless_driver_name_map = { } 
                          collect_drivers.each do |key, value|
                              @@caseless_driver_name_map[key.downcase] = value
                          end
                      end
                      
                      begin
                          require @@caseless_driver_name_map[dc] if @@caseless_driver_name_map[dc]
                      rescue LoadError => e2
                          raise e.class, "Could not find driver #{driver_name} or #{driver_name.downcase} (error: #{e1.message})"
                      end
                  end

                  # On a filesystem that is not case-sensitive (e.g., HFS+ on Mac OS X),
                  # the initial require attempt that loads the driver may succeed even
                  # though the lettercase of driver_name doesn't match the actual
                  # filename. If that happens, const_get will fail and it become
                  # necessary to look though the list of constants and look for a
                  # caseless match.  The result of this match provides the constant
                  # with the proper lettercase -- which can be used to generate the
                  # driver handle.

                  dr = nil
                  begin
                      dr = DBI::DBD.const_get(driver_name.intern)
                  rescue NameError
                      # caseless look for constants to find actual constant
                      dc = driver_name.downcase
                      found = DBI::DBD.constants.find { |e| e.downcase == dc }
                      dr = DBI::DBD.const_get(found.intern) unless found.nil?
                  end
                  
                  # If dr is nil at this point, it means the underlying driver
                  # failed to load.  This usually means it's not installed, but
                  # can fail for other reasons.
                  if dr.nil?
                      err = "Unable to load driver '#{driver_name}'"
                      raise DBI::InterfaceError, err
                  end

                  dbd_dr = dr::Driver.new
                  drh = DBI::DriverHandle.new(dbd_dr)
                  drh.driver_name = dr.driver_name
                  drh.trace(@@trace_mode, @@trace_output)
                  @@driver_map[driver_name] = [drh, dbd_dr]
                  return driver_name 
              else
                  return driver_name
              end
          end
      rescue LoadError, NameError
          if $SAFE >= 1
              raise InterfaceError, "Could not load driver (#{$!.message}). Note that in SAFE mode >= 1, driver URLs have to be case sensitive!"
          else
              raise InterfaceError, "Could not load driver (#{$!.message})"
          end
      end
      
      # Splits a DBI URL into two components - the database driver name
      # and the datasource (along with any options, if any) and returns
      # a two element array, e.g. 'dbi:foo:bar' would return ['foo','bar'].
      #
      # A regular expression is used instead of a simple split to validate
      # the proper format for the URL.  If it isn't correct, an Interface
      # error is raised.
      def parse_url(driver_url)
         if driver_url =~ /^(DBI|dbi):([^:]+)(:(.*))$/ 
            [$2, $4]
         else
            raise InterfaceError, "Invalid Data Source Name"
         end
      end
      
   end # self
      
   #----------------------------------------------------
   #  Fallback classes
   #----------------------------------------------------

   ##
   # Fallback classes for default behavior of DBD driver
   # must be inherited by the DBD driver classes
   #

   class Base
   end

   class BaseDriver < Base

       def initialize(dbd_version)
           major, minor = dbd_version.split(".")
           unless major.to_i == DBD::API_VERSION.split(".")[0].to_i
               raise InterfaceError, "Wrong DBD API version used"
           end
       end

       def connect(dbname, user, auth, attr)
           raise NotImplementedError
       end

       def default_user
           ['', '']
       end

       def default_attributes
           {}
       end

       def data_sources
           []
       end

       def disconnect_all
           raise NotImplementedError
       end

   end # class BaseDriver

   class BaseDatabase < Base

       def initialize(handle, attr)
           @handle = handle
           @attr   = {}
           attr.each {|k,v| self[k] = v} 
       end

       def disconnect
           raise NotImplementedError
       end

       def ping
           raise NotImplementedError
       end

       def prepare(statement)
           raise NotImplementedError
       end

       #============================================
       # OPTIONAL
       #============================================

       def commit
           raise NotSupportedError
       end

       def rollback
           raise NotSupportedError
       end

       def tables
           []
       end

       def columns(table)
           []
       end


       def execute(statement, *bindvars)
           stmt = prepare(statement)
           stmt.bind_params(*bindvars)
           stmt.execute
           stmt
       end

       def do(statement, *bindvars)
           stmt = execute(statement, *bindvars)
           res = stmt.rows
           stmt.finish
           return res
       end

       # includes quote
       include DBI::SQL::BasicQuote

       def [](attr)
           @attr[attr]
       end

       def []=(attr, value)
           raise NotSupportedError
       end

   end # class BaseDatabase

   class BaseStatement < Base

       def initialize(attr=nil)
           @attr = attr || {}
       end



       def bind_param(param, value, attribs)
           raise NotImplementedError
       end

       def execute
           raise NotImplementedError
       end

       def finish
           raise NotImplementedError
       end

       def fetch
           raise NotImplementedError
       end

       ##
       # returns result-set column information as array
       # of hashs, where each hash represents one column
       def column_info
           raise NotImplementedError
       end

       #============================================
       # OPTIONAL
       #============================================

       def bind_params(*bindvars)
           bindvars.each_with_index {|val,i| bind_param(i+1, val, nil) }
           self
       end

       def cancel
       end

       def fetch_scroll(direction, offset)
           case direction
           when SQL_FETCH_NEXT
               return fetch
           when SQL_FETCH_LAST
               last_row = nil
               while (row=fetch) != nil
                   last_row = row
               end
               return last_row
           when SQL_FETCH_RELATIVE
               raise NotSupportedError if offset <= 0
               row = nil
               offset.times { row = fetch; break if row.nil? }
               return row
           else
               raise NotSupportedError
           end
       end

       def fetch_many(cnt)
           rows = []
           cnt.times do
               row = fetch
               break if row.nil?
               rows << row.dup
           end

           if rows.empty?
               nil
           else
               rows
           end
       end

       def fetch_all
           rows = []
           loop do
               row = fetch
               break if row.nil?
               rows << row.dup
           end

           if rows.empty?
               nil
           else
               rows
           end
       end

       def [](attr)
           @attr[attr]
       end

       def []=(attr, value)
           raise NotSupportedError
       end

   end # class BaseStatement
end # module DBI
