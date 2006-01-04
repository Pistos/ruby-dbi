#
# DBD::InterBase
#
# Copyright (c) 2001, 2002 Michael Neumann <neumann@s-direktnet.de>
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
# $Id: InterBase.rb,v 1.1.1.1 2006/01/04 02:03:19 francis Exp $
#

require "interbase"

module DBI
module DBD
module InterBase

VERSION          = "0.1"
USED_DBD_VERSION = "0.1"

IBError = ::InterBase::Error

class Driver < DBI::BaseDriver

  def initialize
    super(USED_DBD_VERSION)
  end

  # database=xxx;[charset=xxx]
  def connect(dbname, user, auth, attr)
    # connect to database
    hash = Utils.parse_params(dbname)

    if hash['database'].nil? 
      raise InterfaceError, "must specify database"
    end

    params = []
    params << hash['charset'] unless hash['charset'].nil? 

    handle = ::InterBase::Connection.connect(hash['database'], user, auth, *params)
    return Database.new(handle, attr)
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

end

class Database < DBI::BaseDatabase
 
  def disconnect
    #@handle.rollback   # is called implicit by #close
    @handle.close
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def ping
    begin
      stmt = execute("SELECT * FROM RDB$RELATIONS")
      stmt.fetch
      stmt.finish
      return true
    rescue IBError
      return false
    end
  end

  def tables
    stmt = execute("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS")  
    rows = stmt.fetch_all || []
    stmt.finish
    rows.collect {|row| row[0]} 
  end

  def prepare(statement)
    Statement.new(@handle.cursor, statement)
  end

=begin
  def []=(attr, value)
    case attr
    when 'AutoCommit'
      if value
        @handle.commiton
      else
        @handle.commitoff
      end
    else
      raise NotSupportedError
    end
    @attr[attr] = value
  end
=end

  def commit
    @handle.commit
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def rollback
    @handle.rollback
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

end # class Database


class Statement < DBI::BaseStatement

  def initialize(cursor, statement)
    @handle = cursor
    @statement = statement
    @params = []
  end

  def bind_param(param, value, attribs)
    raise InterfaceError, "only ? parameters supported" unless param.is_a? Fixnum
    @params[param-1] = value 
  end

  def execute
    @handle.execute(@statement, *@params)
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def finish
    @handle.drop
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def fetch
    @handle.fetch
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def column_info
    retval = []

    @handle.description.each {|col| 
      retval << {'name' => col[0] }
    }
    retval
  rescue IBError => err
    raise DBI::DatabaseError.new(err.message)
  end

  def rows
    nil
  end

end


end # module InterBase
end # module DBD
end # module DBI

