module DBI::DBD::Mysql
    #
    # Models the DBI::BaseStatement API to create DBI::StatementHandle objects.
    # 
    class Statement < DBI::BaseStatement
        include Util

        def initialize(parent, handle, statement, mutex)
            super(nil)

            @parent, @handle, @mutex = parent, handle, mutex
            @params = []

            @prep_stmt = DBI::SQL::PreparedStatement.new(@parent, statement)
        end

        #
        # See DBI::BaseStatement#bind_param. This method will also raise
        # DBI::InterfaceError if +param+ is not a Fixnum, to prevent incorrect
        # binding.
        #
        def bind_param(param, value, attribs)
            raise InterfaceError, "only ? parameters supported" unless param.is_a? Fixnum
            @params[param-1] = value 
        end

        #
        # See DBI::BaseStatement#execute. If DBI thinks this is a query via DBI::SQL.query?(), 
        # it will force the row processed count to 0. Otherwise, it will return
        # what MySQL thinks is the row processed count.
        #
        def execute
            sql = @prep_stmt.bind(@params)
            @mutex.synchronize {
                @handle.query_with_result = true
                @res_handle = @handle.query(sql)
                @column_info = self.column_info
                @current_row = 0
                @rows = DBI::SQL.query?(sql) ? 0 : @handle.affected_rows 
            }
        rescue MyError => err
            error(err)
        end

        def finish
            @res_handle.free if @res_handle
        rescue MyError => err
            error(err)
        end

        #
        # Helper method to aid #fetch. Do not call directly.
        #
        def fill_array(rowdata)
            return nil if rowdata.nil?
            return rowdata.dup
        end

        def fetch
            @current_row += 1
            fill_array(@res_handle.fetch_row)
        rescue MyError => err
            error(err)
        end

        # 
        # See DBI::BaseStatement#fetch_scroll. These additional constants are also supported:
        #
        # * DBI::SQL_FETCH_PRIOR: Fetch the row previous to the current one.
        # * DBI::SQL_FETCH_FIRST: Fetch the first row.
        # * DBI::SQL_FETCH_ABSOLUTE: Fetch the row at the offset provided.
        # * DBI::SQL_FETCH_RELATIVE: Fetch the row at the current point + offset.
        #
        def fetch_scroll(direction, offset)
            case direction
            when DBI::SQL_FETCH_NEXT
                @current_row += 1
                fill_array(@res_handle.fetch_row)
            when DBI::SQL_FETCH_PRIOR
                @res_handle.data_seek(@current_row - 1)
                fill_array(@res_handle.fetch_row)
            when DBI::SQL_FETCH_FIRST
                @current_row = 1
                @res_handle.data_seek(@current_row - 1)
                fill_array(@res_handle.fetch_row)
            when DBI::SQL_FETCH_LAST
                @current_row = @res_handle.num_rows
                @res_handle.data_seek(@current_row - 1)
                fill_array(@res_handle.fetch_row)
            when DBI::SQL_FETCH_ABSOLUTE
                @current_row = offset + 1
                @res_handle.data_seek(@current_row - 1)
                fill_array(@res_handle.fetch_row)
            when DBI::SQL_FETCH_RELATIVE
                @current_row += offset + 1
                @res_handle.data_seek(@current_row - 1)
                fill_array(@res_handle.fetch_row)
            else
                raise NotSupportedError
            end
            #end
        end

        #
        # See DBI::BaseStatement#column_info, and DBI::DBD::Mysql::Database#columns.
        #
        # This method provides all the attributes the +columns+ method
        # provides, and a few others:
        #
        # * mysql_type: These correspond to constants in the Mysql::Types
        #   package, in the lower-level 'mysql' package.
        # * mysql_type_name: A text representation of +mysql_type+. 
        # * mysql_length: The length of the column.
        # * mysql_max_length: The max length of the column. FIXME DESCRIBE
        #   DIFFERENCE
        # * mysql_flags: Internal MySQL flags on this column.
        #
        def column_info
            retval = []

            return [] if @res_handle.nil?

            unique_key_flag = MysqlField.const_get(:UNIQUE_KEY_FLAG)
            multiple_key_flag = MysqlField.const_get(:MULTIPLE_KEY_FLAG)
            indexed = (unique_key_flag | multiple_key_flag)

            # Note: Cannot get 'default' column attribute because MysqlField.def
            # is set only by mysql_list_fields()

            @res_handle.fetch_fields.each {|col| 
                mysql_type_name, dbi_type = Database::TYPE_MAP[col.type] rescue [nil, nil]
                xopen_info = Database::MYSQL_to_XOPEN[mysql_type_name] ||
                    Database::MYSQL_to_XOPEN[nil]
                sql_type = xopen_info[0]
                type_name = DBI::SQL_TYPE_NAMES[sql_type]

                retval << {
                    # Standard Ruby DBI column attributes
                    'name'        => col.name,
                    'sql_type'    => sql_type,
                    'type_name'   => type_name,
                    # XXX it seems mysql counts the literal decimal point when weighing in the "length".
                    'precision'   => type_name == "NUMERIC" ? col.length - 2 : col.length,
                    'scale'       => col.decimals,
                    'nullable'    => !col.is_not_null?,
                    'indexed'     => ((col.flags & indexed) != 0) ||
                                        col.is_pri_key?,
                    'primary'     => col.is_pri_key?,
                    'unique'      => ((col.flags & unique_key_flag) != 0) ||
                                        col.is_pri_key?,
                    # MySQL-specific attributes (signified by leading "mysql_")
                    'mysql_type'       => col.type,
                    'mysql_type_name'  => mysql_type_name,
                    'mysql_length'     => col.length,
                    'mysql_max_length' => col.max_length,
                    'mysql_flags'      => col.flags
                }

                if retval[-1]['sql_type'] == DBI::SQL_TINYINT and retval[-1]['precision'] == 1
                    retval[-1]['dbi_type'] = DBI::Type::Boolean
                elsif dbi_type
                    retval[-1]['dbi_type'] = dbi_type
                end
            }
            retval
        rescue MyError => err
            error(err)
        end

        def rows
            @rows
        end

#                 def []=(attr, value)
#                     case attr
#                     when 'mysql_use_result'
#                         @attr['mysql_store_result'] = ! value
#                         @attr['mysql_use_result']   = value
#                     when 'mysql_store_result'
#                         @attr['mysql_use_result']   = ! value
#                         @attr['mysql_store_result'] = value
#                     else
#                         raise NotSupportedError
#                     end
#                 end

    end # class Statement
end
