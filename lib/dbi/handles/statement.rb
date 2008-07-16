module DBI
   class StatementHandle < Handle

       include Enumerable

       attr_accessor :dbh

       def initialize(handle, fetchable=false, prepared=true)
           super(handle)
           @fetchable = fetchable
           @prepared  = prepared     # only false if immediate execute was used
           @cols = nil
           @coltypes = nil

           # TODO: problems with other DB's?
           #@row = DBI::Row.new(column_names,nil)
           if @fetchable
               @row = DBI::Row.new(column_names, column_types, nil)
           else
               @row = nil
           end
       end

       def finished?
           @handle.nil?
       end

       def fetchable?
           @fetchable
       end

       def bind_param(param, value, attribs=nil)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement wasn't prepared before." unless @prepared
           @handle.bind_param(param, DBI::Utils::ConvParam.conv_param(dbh.driver_name, value)[0], attribs)
       end

       def execute(*bindvars)
           cancel     # cancel before 
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement wasn't prepared before." unless @prepared
           @handle.bind_params(*DBI::Utils::ConvParam.conv_param(dbh.driver_name, *bindvars))
           @handle.execute
           @fetchable = true

           # TODO:?
           #if @row.nil?
           @row = DBI::Row.new(column_names, column_types, nil)
           #end
           return nil
       end

       def finish
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle.finish
           @handle = nil
       end

       def cancel
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle.cancel if @fetchable
           @fetchable = false
       end

       def column_names
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           return @cols unless @cols.nil?
           @cols = @handle.column_info.collect {|col| col['name'] }
       end

       def column_types
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           return @coltypes unless @coltypes.nil?
           @coltypes = @handle.column_info.collect do |col| 
               if col['dbi_type']
                   col['dbi_type']
               else
                   DBI::TypeUtil.type_name_to_module(col['type_name'])
               end
           end
       end

       def column_info
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle.column_info.collect {|col| ColumnInfo.new(col) }
       end

       def rows
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle.rows
       end

       def fetch(&p)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?

           if block_given? 
               while (res = @handle.fetch) != nil
                   @row = @row.dup
                   @row.set_values(res)
                   yield @row
               end
               @handle.cancel
               @fetchable = false
               return nil
           else
               res = @handle.fetch
               if res.nil?
                   @handle.cancel
                   @fetchable = false
               else
                   @row = @row.dup
                   @row.set_values(res)
                   res = @row
               end
               return res
           end
       end

       def each(&p)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable
           raise InterfaceError, "No block given" unless block_given?

           fetch(&p)
       end

       def fetch_array
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable

           if block_given? 
               while (res = @handle.fetch) != nil
                   yield res
               end
               @handle.cancel
               @fetchable = false
               return nil
           else
               res = @handle.fetch
               if res.nil?
                   @handle.cancel
                   @fetchable = false
               end
               return res
           end
       end

       def fetch_hash
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable

           cols = column_names

           if block_given? 
               while (row = @handle.fetch) != nil
                   hash = {}
                   row.each_with_index {|v,i| hash[cols[i]] = v} 
                   yield hash
               end
               @handle.cancel
               @fetchable = false
               return nil
           else
               row = @handle.fetch
               if row.nil?
                   @handle.cancel
                   @fetchable = false
                   return nil
               else
                   hash = {}
                   row.each_with_index {|v,i| hash[cols[i]] = v} 
                   return hash
               end
           end
       end

       def fetch_many(cnt)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable

           cols = column_names
           rows = @handle.fetch_many(cnt)
           if rows.nil?
               @handle.cancel
               @fetchable = false
               return []
           else
               return rows.collect{|r| Row.new(cols, column_types, r)}
           end
       end

       def fetch_all
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable

           cols = column_names
           fetched_rows = []

           begin
               while row = fetch do
                   fetched_rows.push(row)
               end
           rescue Exception
           end

           @handle.cancel
           @fetchable = false

           return fetched_rows
       end

       def fetch_scroll(direction, offset=1)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           raise InterfaceError, "Statement must first be executed" unless @fetchable

           row = @handle.fetch_scroll(direction, offset)
           if row.nil?
               #@handle.cancel
               #@fetchable = false
               return nil
           else
               @row.set_values(row)
               return @row
           end
       end

       def [] (attr)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle[attr]
       end

       def []= (attr, val)
           raise InterfaceError, "Statement was already closed!" if @handle.nil?
           @handle[attr] = val
       end

   end # class StatementHandle
end
