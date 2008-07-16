module DBI
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
end
