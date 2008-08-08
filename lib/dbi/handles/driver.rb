module DBI
   class DriverHandle < Handle

       attr_writer :driver_name

       def connect(db_args, user, auth, params)

           user = @handle.default_user[0] if user.nil?
           auth = @handle.default_user[1] if auth.nil?

           # TODO: what if only one of them is nil?
           #if user.nil? and auth.nil? then
           #  user, auth = @handle.default_user
           #end

           params ||= {}
           new_params = @handle.default_attributes
           params.each {|k,v| new_params[k] = v} 

           if params.has_key?(:_convert_types)
               @convert_types = params[:_convert_types]
           end

           db = @handle.connect(db_args, user, auth, new_params)
           dbh = DatabaseHandle.new(db, @convert_types)
           # FIXME trace
           # dbh.trace(@trace_mode, @trace_output)
           dbh.driver_name = @driver_name

           if block_given?
               begin
                   yield dbh
               ensure  
                   dbh.disconnect if dbh.connected?
               end  
           else
               return dbh
           end
       end

       def data_sources
           @handle.data_sources
       end

       def disconnect_all
           @handle.disconnect_all
       end
   end
end
