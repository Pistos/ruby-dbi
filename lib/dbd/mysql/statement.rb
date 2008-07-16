module DBI::DBD::Mysql
    class Statement < DBI::BaseStatement
        include Util

        def initialize(parent, handle, statement, mutex)
            super(nil)

            @parent, @handle, @mutex = parent, handle, mutex
            @params = []

            @prep_stmt = DBI::SQL::PreparedStatement.new(@parent, statement)
        end

        def bind_param(param, value, attribs)
            raise InterfaceError, "only ? parameters supported" unless param.is_a? Fixnum
            @params[param-1] = value 
        end

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

        def fill_array(rowdata)
            return nil if rowdata.nil?
            row = []
            rowdata.each_with_index { |value, index|
                info = @column_info[index]
                type = info['mysql_type']
                type_symbol =
                    if DBI::SQL_TINYINT == info['sql_type'] and 1 == info['precision']
                        DBI::Type::Boolean
                    else
                        Database::TYPE_MAP[type][1] || DBI::Type::Varchar rescue DBI::Type::Varchar
                    end
                row[index] = type_symbol.parse(value)
            }
            row
        end

        def fetch
            @current_row += 1
            fill_array(@res_handle.fetch_row)
        rescue MyError => err
            error(err)
        end

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

        def column_info
            retval = []

            return [] if @res_handle.nil?

            unique_key_flag = MysqlField.const_get(:UNIQUE_KEY_FLAG)
            multiple_key_flag = MysqlField.const_get(:MULTIPLE_KEY_FLAG)
            indexed = (unique_key_flag | multiple_key_flag)

            # Note: Cannot get 'default' column attribute because MysqlField.def
            # is set only by mysql_list_fields()

            @res_handle.fetch_fields.each {|col| 
                mysql_type_name = Database::TYPE_MAP[col.type][0] rescue nil
                xopen_info = Database::MYSQL_to_XOPEN[mysql_type_name] ||
                    Database::MYSQL_to_XOPEN[nil]
                sql_type = xopen_info[0]
                type_name = DBI::SQL_TYPE_NAMES[sql_type]

                retval << {
                    # Standard Ruby DBI column attributes
                              'name'        => col.name,
                              'sql_type'    => sql_type,
                              'type_name'   => type_name,
                              'precision'   => col.length,
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
            }
            retval
        rescue MyError => err
            error(err)
        end

        def rows
            @rows
        end

=begin
                def []=(attr, value)
                    case attr
                    when 'mysql_use_result'
                        @attr['mysql_store_result'] = ! value
                        @attr['mysql_use_result']   = value
                    when 'mysql_store_result'
                        @attr['mysql_use_result']   = ! value
                        @attr['mysql_store_result'] = value
                    else
                        raise NotSupportedError
                    end
                end
=end

    end # class Statement
end
