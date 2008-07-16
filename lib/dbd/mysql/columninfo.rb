# The MySQL-specific column_info method defines some MySQL-specific
# members of the column info attribute array. Extend ColumnInfo by
# adding methods so that info["mysql_x"], for each MySQL-specific x, also
# has get and set info.mysql_x and info.mysql_x= methods.

class ColumnInfo

    # Get the column's MySQL type code
    def mysql_type
        self['mysql_type']
    end

    # Set the column's MySQL type code
    def mysql_type=(val)
        self['mysql_type'] = val
    end

    # Get the column's MySQL type name
    def mysql_type_name
        self['mysql_type_name']
    end

    # Set the column's MySQL type name
    def mysql_type_name=(val)
        self['mysql_type_name'] = val
    end

    # Get the column's MySQL length
    def mysql_length
        self['mysql_length']
    end

    # Set the column's MySQL length
    def mysql_length=(val)
        self['mysql_length'] = val
    end

    # Get the column's MySQL max length
    def mysql_max_length
        self['mysql_max_length']
    end

    # Set the column's MySQL max length
    def mysql_max_length=(val)
        self['mysql_max_length'] = val
    end

    # Get the column's MySQL flags
    def mysql_flags
        self['mysql_flags']
    end

    # Set the column's MySQL flags
    def mysql_flags=(val)
        self['mysql_flags'] = val
    end

end # class ColumnInfo
