############################################################
# tc_columninfo.rb
#
# Test case for the ColumnInfo class.
############################################################
$LOAD_PATH.unshift(Dir.pwd)
$LOAD_PATH.unshift(File.dirname(Dir.pwd))
$LOAD_PATH.unshift("../../lib")
$LOAD_PATH.unshift("../../lib/dbi")
$LOAD_PATH.unshift("lib")

require "columninfo"
require "test/unit"

class TC_DBI_ColumnInfo < Test::Unit::TestCase
   def setup
      @colinfo = ColumnInfo.new
   end

   def test_name
      assert_respond_to(@colinfo, :name)
      assert_respond_to(@colinfo, :name=)
   end

   def test_sql_type
      assert_respond_to(@colinfo, :sql_type)
      assert_respond_to(@colinfo, :sql_type=)
   end

   def test_type_name
      assert_respond_to(@colinfo, :type_name)
      assert_respond_to(@colinfo, :type_name=)
   end

   def test_precision
      assert_respond_to(@colinfo, :precision)
      assert_respond_to(@colinfo, :precision=)
      assert_respond_to(@colinfo, :size)     
      assert_respond_to(@colinfo, :size=)
      assert_respond_to(@colinfo, :length)
      assert_respond_to(@colinfo, :length=)
   end

   def test_scale
      assert_respond_to(@colinfo, :scale)
      assert_respond_to(@colinfo, :scale=)
      assert_respond_to(@colinfo, :decimal_digits)
      assert_respond_to(@colinfo, :decimal_digits=)
   end

   def test_default
      assert_respond_to(@colinfo, :default)
      assert_respond_to(@colinfo, :default=)
   end

   def test_nullable
      assert_respond_to(@colinfo, :nullable)
      assert_respond_to(@colinfo, :nullable?)
      assert_respond_to(@colinfo, :nullable=)
   end

   def test_indexed
      assert_respond_to(@colinfo, :indexed)
      assert_respond_to(@colinfo, :indexed?)
      assert_respond_to(@colinfo, :indexed=)
   end

   def test_primary
      assert_respond_to(@colinfo, :primary)
      assert_respond_to(@colinfo, :primary?)
      assert_respond_to(@colinfo, :primary=)
   end

   def test_unique
      assert_respond_to(@colinfo, :unique)
      assert_respond_to(@colinfo, :unique?)
      assert_respond_to(@colinfo, :is_unique)
      assert_respond_to(@colinfo, :unique=)
   end

   def teardown
      @colinfo = nil
   end
end
