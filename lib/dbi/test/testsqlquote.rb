#!/usr/local/bin/ruby
# Ruby Unit Tests

require 'runit/testcase'
require 'runit/cui/testrunner'

require 'require_dispatch'
require "dbi"

class MockSql
  include DBI::SQL::BasicQuote
end

$last_suite = RUNIT::TestSuite.new

# --------------------------------------------------------------------

class TestSqlQuote < RUNIT::TestCase
  def setup
    @sql = MockSql.new
  end

  def teardown
    @sql = nil
  end

  def test_quoting
    assert_equal "'HI'", @sql.quote("HI")
    assert_equal "'Two Words'", @sql.quote("Two Words")
    assert_equal "1", @sql.quote(1)
  end

  def test_embeded_quotes
    assert_equal "''''", @sql.quote("'")
  end

  def test_embedded_back_slashes
    assert_equal "'\\'", @sql.quote("\\")
  end

  def test_advanced_quoting
    assert_equal "'don''t'", @sql.quote("don't")
    assert_equal "'I won''t and I can''t'", @sql.quote("I won't and I can't")
    assert_equal "'c:\\bin\\program'", @sql.quote("c:\\bin\\program")
  end

  def test_array_quoting
    assert_equal "'Hi''ya', 'World', 123", @sql.quote(["Hi'ya", "World", 123])
  end

  def test_nil_quoting
    assert_equal 'NULL', @sql.quote(nil)
  end

  def test_time_quoting
    tm = Time.at(1084995693)
    assert_equal("'#{ tm.rfc2822 }'", @sql.quote(tm))
  end

end

$last_suite.add_test(TestSqlQuote.suite)



# --------------------------------------------------------------------

if __FILE__ == $0 then
  RUNIT::CUI::TestRunner.quiet_mode = false
  RUNIT::CUI::TestRunner.run($last_suite)
end
