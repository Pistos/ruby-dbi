module DBI
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
           raise NotSupportedError
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

       def [](attr)
           @attr[attr]
       end

       def []=(attr, value)
           raise NotSupportedError
       end
   end # class BaseDatabase
end
