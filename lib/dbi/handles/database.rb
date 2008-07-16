module DBI
   class DatabaseHandle < Handle

       def driver_name
           return @driver_name.dup if @driver_name
           return nil
       end

       def driver_name=(name)
           @driver_name = name
           @driver_name.freeze
       end

       def connected?
           not @handle.nil?
       end

       def disconnect
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.disconnect
           @handle = nil
       end

       def prepare(stmt)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           sth = StatementHandle.new(@handle.prepare(stmt), false)
           sth.trace(@trace_mode, @trace_output)
           sth.dbh = self

           if block_given?
               begin
                   yield sth
               ensure
                   sth.finish unless sth.finished?
               end
           else
               return sth
           end 
       end

       def execute(stmt, *bindvars)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           sth = StatementHandle.new(@handle.execute(stmt, *DBI::Utils::ConvParam.conv_param(driver_name, *bindvars)), true, false)
           sth.trace(@trace_mode, @trace_output)
           sth.dbh = self

           if block_given?
               begin
                   yield sth
               ensure
                   sth.finish unless sth.finished?
               end
           else
               return sth
           end 
       end

       def do(stmt, *bindvars)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.do(stmt, *DBI::Utils::ConvParam.conv_param(driver_name, *bindvars))
       end

       def select_one(stmt, *bindvars)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           row = nil
           execute(stmt, *bindvars) do |sth|
               row = sth.fetch 
           end
           row
       end

       def select_all(stmt, *bindvars, &p)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           rows = nil
           execute(stmt, *bindvars) do |sth|
               if block_given?
                   sth.each(&p)
               else
                   rows = sth.fetch_all 
               end
           end
           return rows
       end

       def tables
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.tables
       end

       def columns( table )
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.columns( table ).collect {|col| ColumnInfo.new(col) }
       end

       def ping
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.ping
       end

       def quote(value)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.quote(value)
       end

       def commit
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.commit
       end

       def rollback
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle.rollback
       end

       def transaction
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           raise InterfaceError, "No block given" unless block_given?

           commit
           begin
               yield self
               commit
           rescue Exception
               rollback
               raise
           end
       end

       def [] (attr)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle[attr]
       end

       def []= (attr, val)
           raise InterfaceError, "Database connection was already closed!" if @handle.nil?
           @handle[attr] = val
       end
   end
end
