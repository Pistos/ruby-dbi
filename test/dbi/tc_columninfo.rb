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
      @colinfo = ColumnInfo.new(
         "name"      => "test",
         "sql_type"  => "numeric",
         "type_name" => "test_type_name",
         "precision" => 2,
         "scale"     => 2,
         "default"   => 100.00,
         "nullable"  => false,
         "indexed"   => true,
         "primary"   => true,
         "unique"    => false
      )
      @keys = %w/name sql_type type_name precision scale default nullable
         indexed primary unique
      /
   end
   
   def test_constructor
      assert_nothing_raised{ ColumnInfo.new }
      assert_raises(TypeError){ ColumnInfo.new(1) }
   end

   def test_name_basic
      assert_respond_to(@colinfo, :name)
      assert_respond_to(@colinfo, :name=)
   end

   def test_name
      assert_equal("test", @colinfo["name"])
      assert_equal("test", @colinfo.name)
   end

   def test_sql_type_basic
      assert_respond_to(@colinfo, :sql_type)
      assert_respond_to(@colinfo, :sql_type=)
   end
   
   def test_sql_type
      assert_equal("numeric", @colinfo["sql_type"])
      assert_equal("numeric", @colinfo.sql_type)
   end

   def test_type_name_basic
      assert_respond_to(@colinfo, :type_name)
      assert_respond_to(@colinfo, :type_name=)
   end

   def test_type_name
      assert_equal("test_type_name", @colinfo["type_name"])
      assert_equal("test_type_name", @colinfo.type_name)
   end

   def test_precision_basic
      assert_respond_to(@colinfo, :precision)
      assert_respond_to(@colinfo, :precision=)
      assert_respond_to(@colinfo, :size)     
      assert_respond_to(@colinfo, :size=)
      assert_respond_to(@colinfo, :length)
      assert_respond_to(@colinfo, :length=)
   end

   def test_precision
      assert_equal(2, @colinfo["precision"])
      assert_equal(2, @colinfo.precision)
   end

   def test_scale_basic
      assert_respond_to(@colinfo, :scale)
      assert_respond_to(@colinfo, :scale=)
      assert_respond_to(@colinfo, :decimal_digits)
      assert_respond_to(@colinfo, :decimal_digits=)
   end

   def test_scale
      assert_equal(2, @colinfo["scale"])
      assert_equal(2, @colinfo.scale)
   end

   def test_default_basic
      assert_respond_to(@colinfo, :default)
      assert_respond_to(@colinfo, :default=)
   end

   def test_default
      assert_equal(100.00, @colinfo["default"])
      assert_equal(100.00, @colinfo.default)
   end

   def test_nullable_basic
      assert_respond_to(@colinfo, :nullable)
      assert_respond_to(@colinfo, :nullable?)
      assert_respond_to(@colinfo, :nullable=)
   end

   def test_nullable
      assert_equal(false, @colinfo["nullable"])
      assert_equal(false, @colinfo.nullable)
   end

   def test_indexed_basic
      assert_respond_to(@colinfo, :indexed)
      assert_respond_to(@colinfo, :indexed?)
      assert_respond_to(@colinfo, :indexed=)
   end

   def test_indexed
      assert_equal(true, @colinfo["indexed"])
      assert_equal(true, @colinfo.indexed)
   end

   def test_primary_basic
      assert_respond_to(@colinfo, :primary)
      assert_respond_to(@colinfo, :primary?)
      assert_respond_to(@colinfo, :primary=)
   end

   def test_primary
      assert_equal(true, @colinfo["primary"])
      assert_equal(true, @colinfo.primary)
   end

   def test_unique_basic
      assert_respond_to(@colinfo, :unique)
      assert_respond_to(@colinfo, :unique?)
      assert_respond_to(@colinfo, :is_unique)
      assert_respond_to(@colinfo, :unique=)
   end

   def test_unique
      assert_equal(false, @colinfo["unique"])
      assert_equal(false, @colinfo.unique)
   end
   
   def test_keys
      assert_respond_to(@colinfo, :keys)
      assert_equal(@keys.sort, @colinfo.keys.sort)
   end
   
   def test_respond_to_hash_methods
      assert_respond_to(@colinfo, :each)
      assert_respond_to(@colinfo, :empty?)
      assert_respond_to(@colinfo, :has_key?)
   end

   def teardown
      @colinfo = nil
   end
end
