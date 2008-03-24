# each type has it's own class, with two methods, register_conversion and
# parse.  these are basically facades to coerce a specific underlying type
# (generally Something -> String or String -> Something) 
#
# parse parses a string into the native represented type.
#
# register_conversion converts the native type into it's string representation
# for quoting in sql calls. Since conversions are different for each DBD, the
# DBD registers a conversion method based on it's DBD.

# Example of writing a type:

module DBI::Type::Integer
    def self.parse(obj)
        return obj.to_i if obj.respond_to? :to_i
        return obj
    end
end

# now, in the Mysql DBD:
DBI::Type::Integer.register_conversion(DBI::DBD::Mysql) do |obj|
    return obj.to_s if obj.respond_to? :to_s
    return obj.to_str if obj.respond_to? :to_str
    return obj
end

# Of course there will be base classes that do some of these, and there's
# absolutely no reason that these could not be modules, but classes with
# inheritable methods or leverage mixins. I'm just not sure what'd be easiest,
# yet.

# DBI should do most of the conversion, so we're going to facilitate this by
# adding an extra field to the DatabaseHandle and StatementHandle #columns
# methods which will provide these module/class names to DBI which will perform
# the conversions on the fly. These should (ideally) be able to have overrides,
# see bind_coltype below.

# example:

sth = dbh.prepare("select my_timestamp from timestamp_table")
sth.execute # obtains type information from columns() method
row = sth.fetch
assert_kind_of(DateTime, row[0])
assert_equal(
    {
        [
            # lots of stuff omitted
            :name => "my_timestamp",
            :type => DBI::Type::Timestamp
        ]
    }, sth.columns
)
sth.finish

# bind_coltype binds a column type so that successive fetches return that type instead.
# as with all types, a parse implementation must work otherwise we'll need to
# chuck an exception. The type will be parsed from String, not from the
# original destination type.

# example:

sth = dbh.prepare("select my_timestamp from timestamp_table")
sth.execute
sth.bind_coltype(1, DBI::Type::Timestamp)
row = sth.fetch
assert_kind_of(DateTime, row[0])
# this can be called between fetches. throwing it 'nil' means no conversion will happen
sth.bind_coltype(1, nil)
row = sth.fetch
assert_kind_of(String, row[0])
sth.finish

# bind_coltype will also work for inbound parameters (which is the scope of another document):
sth = dbh.prepare("insert into timestamp_table (my_timestamp) values (?)")
sth.bind_coltype(1, DBI::Type::Integer) # epoch?
sth.execute(Time.now)
sth.finish
