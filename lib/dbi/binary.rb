module DBI
   # TODO: do we need Binary?
   # perhaps easier to call #bind_param(1, binary_string, 'type' => SQL_BLOB)
   class Binary
      attr_accessor :data
      def initialize(data)
         @data = data
      end
      
      def to_s
         @data
      end
   end
end
