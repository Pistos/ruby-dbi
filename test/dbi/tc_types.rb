$LOAD_PATH.unshift(Dir.pwd)
$LOAD_PATH.unshift(File.dirname(Dir.pwd))
$LOAD_PATH.unshift("../../lib")
$LOAD_PATH.unshift("../../lib/dbi")
$LOAD_PATH.unshift("lib")

require "dbi"
require "test/unit"

class MyType
    def initialize(obj)
        @obj = obj
    end

    def to_s
        @obj.to_s
    end
end

class TC_DBI_Type < Test::Unit::TestCase
    def test_stub
        assert true
    end
end

class TC_DBI_TypeUtil < Test::Unit::TestCase
    def cast(obj, driver_name=nil)
        DBI::TypeUtil.convert(driver_name, obj)
    end

    def datecast(obj)
        "'#{::DateTime.parse(obj.to_s).strftime("%m/%d/%Y %H:%M:%S")}'"
    end

    def test_default_unknown_cast
        assert_kind_of(String, cast(MyType.new("foo")))
        assert_equal("'foo'", cast(MyType.new("foo")))
    end

    def test_default_numeric_cast
        assert_kind_of(String, cast(1))
        assert_equal("1", cast(1))
    end

    def test_default_string_cast
        assert_kind_of(String, cast("foo"))
        assert_equal("'foo'", cast("foo"))
        assert_equal("'foo''bar'", cast("foo'bar"))
    end

    def test_default_time_casts
        assert_kind_of(String, cast(Time.now))
        assert_kind_of(String, cast(Date.today))
        assert_kind_of(String, cast(DateTime.now))
      
        obj = Time.now
        assert_equal(datecast(obj), cast(obj))
        obj = Date.today
        assert_equal(datecast(obj), cast(obj))
        obj = DateTime.now
        assert_equal(datecast(obj), cast(obj))
    end

    def test_default_boolean_casts
        assert_kind_of(String, cast(false))
        assert_kind_of(String, cast(true))
        assert_kind_of(String, cast(nil))

        assert_equal("'1'", cast(true))
        assert_equal("'0'", cast(false))
        assert_equal("'NULL'", cast(nil))
    end

    def test_default_binary_casts
        assert_kind_of(DBI::Binary, cast(DBI::Binary.new("poop")))
        obj = DBI::Binary.new("poop")
        assert_equal(obj.object_id, cast(obj).object_id)
    end
end
