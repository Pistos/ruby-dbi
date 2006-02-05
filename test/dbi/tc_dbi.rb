######################################################################
# tc_dbi.rb
#
# Test case for the DBI module (dbi.rb).
######################################################################
$LOAD_PATH.unshift(Dir.pwd)
$LOAD_PATH.unshift(File.dirname(Dir.pwd))
$LOAD_PATH.unshift("../../lib")
$LOAD_PATH.unshift("../../lib/dbi")
$LOAD_PATH.unshift("lib")

require 'dbi'
require 'test/unit'

class TC_DBI < Test::Unit::TestCase
   def setup
      @db_error = DBI::DatabaseError.new("test", 1, "montana")
   end

   def test_dbi_version
      assert_equal("0.1.0", DBI::VERSION)
   end

   def test_dbd_module
      assert_equal("dbi/dbd", DBI::DBD::DIR)
      assert_equal("0.3", DBI::DBD::API_VERSION)
   end

   def test_fetch_scroll_constants
      assert_equal(1, DBI::SQL_FETCH_NEXT)
      assert_equal(2, DBI::SQL_FETCH_PRIOR)
      assert_equal(3, DBI::SQL_FETCH_FIRST)
      assert_equal(4, DBI::SQL_FETCH_LAST)
      assert_equal(5, DBI::SQL_FETCH_ABSOLUTE)
      assert_equal(6, DBI::SQL_FETCH_RELATIVE)
   end

   def test_sql_type_constants
      assert_equal(1, DBI::SQL_CHAR)
      assert_equal(2, DBI::SQL_NUMERIC)
      assert_equal(3, DBI::SQL_DECIMAL)
      assert_equal(4, DBI::SQL_INTEGER)
      assert_equal(5, DBI::SQL_SMALLINT)
      assert_equal(6, DBI::SQL_FLOAT)
      assert_equal(7, DBI::SQL_REAL)
      assert_equal(8, DBI::SQL_DOUBLE)
      assert_equal(9, DBI::SQL_DATE)
      assert_equal(10, DBI::SQL_TIME)
      assert_equal(11, DBI::SQL_TIMESTAMP)
      assert_equal(12, DBI::SQL_VARCHAR)
      assert_equal(100, DBI::SQL_OTHER)
      assert_equal(-1, DBI::SQL_LONGVARCHAR)
      assert_equal(-2, DBI::SQL_BINARY)
      assert_equal(-3, DBI::SQL_VARBINARY)
      assert_equal(-4, DBI::SQL_LONGVARBINARY)
      assert_equal(-5, DBI::SQL_BIGINT)
      assert_equal(-6, DBI::SQL_TINYINT)
      assert_equal(-7, DBI::SQL_BIT)
      assert_equal(-10, DBI::SQL_BLOB)
      assert_equal(-11, DBI::SQL_CLOB)
   end

   def test_sql_type_names
      assert_equal('BIT', DBI::SQL_TYPE_NAMES[DBI::SQL_BIT])
      assert_equal('TINYINT', DBI::SQL_TYPE_NAMES[DBI::SQL_TINYINT])
      assert_equal('SMALLINT', DBI::SQL_TYPE_NAMES[DBI::SQL_SMALLINT])
      assert_equal('INTEGER', DBI::SQL_TYPE_NAMES[DBI::SQL_INTEGER])
      assert_equal('BIGINT', DBI::SQL_TYPE_NAMES[DBI::SQL_BIGINT])
      assert_equal('FLOAT', DBI::SQL_TYPE_NAMES[DBI::SQL_FLOAT])
      assert_equal('REAL', DBI::SQL_TYPE_NAMES[DBI::SQL_REAL])
      assert_equal('DOUBLE', DBI::SQL_TYPE_NAMES[DBI::SQL_DOUBLE])
      assert_equal('NUMERIC', DBI::SQL_TYPE_NAMES[DBI::SQL_NUMERIC])
      assert_equal('DECIMAL', DBI::SQL_TYPE_NAMES[DBI::SQL_DECIMAL])
      assert_equal('CHAR', DBI::SQL_TYPE_NAMES[DBI::SQL_CHAR])
      assert_equal('VARCHAR', DBI::SQL_TYPE_NAMES[DBI::SQL_VARCHAR])
      assert_equal('LONG VARCHAR', DBI::SQL_TYPE_NAMES[DBI::SQL_LONGVARCHAR])
      assert_equal('DATE', DBI::SQL_TYPE_NAMES[DBI::SQL_DATE])
      assert_equal('TIME', DBI::SQL_TYPE_NAMES[DBI::SQL_TIME])
      assert_equal('TIMESTAMP', DBI::SQL_TYPE_NAMES[DBI::SQL_TIMESTAMP])
      assert_equal('BINARY', DBI::SQL_TYPE_NAMES[DBI::SQL_BINARY])
      assert_equal('VARBINARY', DBI::SQL_TYPE_NAMES[DBI::SQL_VARBINARY])
      assert_equal('LONG VARBINARY',
         DBI::SQL_TYPE_NAMES[DBI::SQL_LONGVARBINARY])
      assert_equal('BLOB', DBI::SQL_TYPE_NAMES[DBI::SQL_BLOB])
      assert_equal('CLOB', DBI::SQL_TYPE_NAMES[DBI::SQL_CLOB])
      assert_equal(nil, DBI::SQL_TYPE_NAMES[DBI::SQL_OTHER])
   end

   def test_dbi_exception_classes
      assert(DBI::Error)
      assert(DBI::Warning)
      assert(DBI::InterfaceError)
      assert(DBI::NotImplementedError)
      assert(DBI::DatabaseError)
      assert(DBI::DataError)
      assert(DBI::OperationalError)
      assert(DBI::IntegrityError)
      assert(DBI::InternalError)
      assert(DBI::ProgrammingError)
      assert(DBI::NotSupportedError)
   end

   # This error class gets extra testing since it defines some custom
   # accessors.
   def test_dbi_database_error
      assert_respond_to(@db_error, :errstr)
      assert_respond_to(@db_error, :err)
      assert_respond_to(@db_error, :state)
      assert_equal("test", @db_error.errstr)
      assert_equal(1, @db_error.err)
      assert_equal("montana", @db_error.state)
   end

   def teardown
      @db_error = nil
   end
end
