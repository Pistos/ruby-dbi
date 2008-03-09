require File.join(File.dirname(__FILE__), 'base')
require 'dbd/Pg'

######################################################################
# Test the PostgreSql DBD driver.  This test exercises options
# difficult to test through the standard DBI interface.
#
class TestDbdPostgres < PGUnitBase

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

    def test_columns
        [
            {
                    "name"=>"name",
                    "default"=>nil,
                    "primary"=>nil,
                    "scale"=>nil,
                    "sql_type"=>12,
                    "nullable"=>false,
                    "indexed"=>false,
                    "precision"=>255,
                    "type_name"=>"character varying",
                    "unique"=>nil
            },
            {
                    "name"=>"age",
                    "default"=>nil,
                    "primary"=>nil,
                    "scale"=>nil,
                    "sql_type"=>4,
                    "nullable"=>false,
                    "indexed"=>false,
                    "precision"=>4,
                    "type_name"=>"integer",
                    "unique"=>nil
            }
        ], @dbh.columns("names")
    end

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

  def test_tables_call
      # per bug #1082, views do not show up in tables listing.
      assert get_dbi.tables.include?("view_names")
  end
  
  def get_dbi
      config = DBDConfig.get_config
      DBI.connect("dbi:Pg:#{config['postgresql']['dbname']}", config['postgresql']['username'], config['postgresql']['password'])
  end

  def get_dbd
      config = DBDConfig.get_config['postgresql']
      result = DBI::DBD::Pg::Database.new(config['dbname'], config['username'], config['password'], {})
      result['AutoCommit'] = true
      result
  end

  def test_timestamp
      ts = nil

      assert_nothing_raised do
          sth = @dbh.prepare("insert into time_test (mytimestamp) values (?)")
          ts = DBI::Timestamp.new(Time.now)
          ts.fraction = 1
          sth.execute(ts)
          sth.finish
      end

      assert_nothing_raised do
          sth = @dbh.prepare("select * from time_test")
          sth.execute
          row = sth.fetch
          sth.finish
          assert_equal [nil, ts], row
      end
  end
end

# --------------------------------------------------------------------
