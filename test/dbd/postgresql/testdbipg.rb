#!/usr/bin/env ruby
# Ruby Unit Tests

require 'test/unit'
require 'DBD/Pg/Pg'

######################################################################
# Test the PostgreSql DBD driver.  This test exercises options
# difficult to test through the standard DBI interface.
#
class TestDbdPostgres < Test::Unit::TestCase

    # FIXME this is a feature that should be there, but currently isn't.
#   def test_connect
#     dbd = get_dbd
#     assert_not_nil dbd.connection
#     assert_equal 'localhost', dbd.connection.host
#     assert_equal 'erikh', dbd.connection.user
#     assert_equal 'rubytest', dbd.connection.db
#     assert_equal 5432, dbd.connection.port
#   ensure
#      dbd.disconnect if dbd
#   end

  def test_connect_errors
    dbd = nil
    ex = assert_raise(DBI::OperationalError) {
      dbd = DBI::DBD::Pg::Database.new('rubytest:1234', 'jim', nil, {})
    }
    ex = assert_raise(DBI::OperationalError) {
      dbd = DBI::DBD::Pg::Database.new('bad_db_name', 'jim', nil, {})
    }
  ensure
    dbd.disconnect if dbd
  end

  def test_type_map
    dbd = get_dbd
    def dbd.type_map
      @type_map
    end
    assert dbd.type_map
    assert_equal 21, dbd.convert("21", 23)
    assert_equal "21", dbd.convert("21", 1043)
    assert_equal 21.5, dbd.convert("21.5", 701)
  end

  def test_simple_command
    dbd = get_dbd
    res = dbd.do("INSERT INTO names (name, age) VALUES('Dan', 16)")
    assert_equal 1, res
    
    sth = get_dbi.prepare("SELECT name FROM names WHERE age=16")
    sth.execute
    assert sth.fetchable?
    # XXX FIXME This is a bug in the DBD. #rows should equal 1 for select statements.
    assert_equal 0, sth.rows
  ensure
    dbd.do("DELETE FROM names WHERE age < 20")
    dbd.disconnect if dbd
  end

  def test_bad_command
    dbd = get_dbd
    assert_raise(DBI::ProgrammingError) {
      dbd.do("INSERT INTO bad_table (name, age) VALUES('Dave', 12)")
    }
  ensure
    dbd.disconnect if dbd
  end

  def test_query_single
    dbd = get_dbd
    res = dbd.prepare("SELECT name, age FROM names WHERE age=21;")
    assert res
    res.execute
    fields = res.column_info
    assert_equal 2, fields.length
    assert_equal 'name', fields[0]['name']
    assert_equal 'age', fields[1]['name']

    row = res.fetch

    assert_equal 'Bob', row[0]
    assert_equal 21, row[1]

    row = res.fetch
    assert_nil row

    res.finish
  ensure
    dbd.disconnect if dbd
  end

  def test_query_multi
    dbd = get_dbd
    res = dbd.prepare("SELECT name, age FROM names WHERE age > 20;")

    expected_list = ['Bob', 'Charlie']
    res.execute
    while row=res.fetch
      expected = expected_list.shift
      assert_equal expected, row[0]
    end

    res.finish
  ensure
    dbd.disconnect if dbd
  end
  
  def setup
      system "psql rubytest < dump.sql >>sql.log"
  end

  def teardown
      system "psql rubytest < drop_tables.sql >>sql.log"
  end

  private # ----------------------------------------------------------

  def get_dbi
    dbh = DBI.connect("dbi:Pg:rubytest", "erikh", "monkeys")
    dbh
  end

  def get_dbd
    result = DBI::DBD::Pg::Database.new('rubytest', 'erikh', 'monkeys', {})
    result['AutoCommit'] = true
    result
  end
  
end

# --------------------------------------------------------------------

if __FILE__ == $0 then
    require 'test/unit/ui/console/testrunner'
    require 'dbi'
    Test::Unit::UI::Console::TestRunner.run(TestDbdPostgres)
end

