#!/usr/local/bin/ruby
# Ruby Unit Tests

require 'runit/testcase'
require 'runit/cui/testrunner'

require 'dbi'

######################################################################
# Configuration Control
#
class DbiTestConfig
  attr_accessor :driver, :dbname, :user, :password
  attr_accessor :tables
  attr_accessor :setup, :teardown
  
  def id
    "DBI:#{driver}:#{dbname}"
  end

  def load(fn)
    @tables = []
    open(fn) { |file|
      file.each { |line|
	next if line =~ /^\s*#/	
	if line =~ /^\s*(\S+)\s+(.*)$/
	  key, value = $1, $2
	  case key.downcase
	  when 'driver'
	    @driver = value
	  when 'dbname'
	    @dbname = value
	  when 'user'
	    @user = value
	  when 'password'
	    @password = value
	  when 'table'
	    @tables << value
	  when 'setup'
	    @setup = value
	  when 'teardown'
	    @teardown = value
	  else
	    raise "Unidentified Config Item '#{line}'"
	  end
	end
      }
    }
  end

  def DbiTestConfig.load(fn)
    cfg = DbiTestConfig.new
    cfg.load(fn)
    cfg
  end
  
end


######################################################################
# Configuration Options
#
# Set the following values as appropriate to test the driver.
#
module DbiTestUtils
  def get_db
    db = DBI.connect($cfg.id, $cfg.user, $cfg.password)
    assert db.connected?
    db
  end

  def get_db_auto
    db = get_db
    db['AutoCommit'] = true
    db
  end

  def get_db_tran
    db = get_db
    db['AutoCommit'] = false
    db
  end
end


module DbiInsertUtils

  def check_insert
    st = @db.prepare("SELECT name, age FROM #{$cfg.tables[0]} WHERE age = 12")
    st.execute
    row = st.fetch
    assert_equal 'Zeb', row[0]
    assert_equal 12, row[1]
    assert_nil st.fetch
  ensure
    st.finish if st
  end

  def check_no_insert
    st = @db.prepare("SELECT name, age FROM #{$cfg.tables[0]} WHERE age = 12")
    st.execute
    assert_nil st.fetch
  ensure
    st.finish if st
  end

  def clear_insert
    @db.do("DELETE FROM #{$cfg.tables[0]} WHERE age < 20 OR name = 'Zeb'")
  end

end

$last_suite = RUNIT::TestSuite.new


######################################################################
class TestDbiAttributes < RUNIT::TestCase

  include DbiTestUtils
  include DbiInsertUtils

  def test_auto_commit
    db = get_db
    assert db['AutoCommit']
    db['AutoCommit'] = false
    assert ! db['AutoCommit']
    db['AutoCommit'] = true
    assert db['AutoCommit']
  ensure
    db.disconnect if db
  end
end

$last_suite.add_test(TestDbiAttributes.suite)


######################################################################
class TestDbiDb < RUNIT::TestCase

  include DbiTestUtils
  include DbiInsertUtils

  def setup
    @db = get_db_auto
  end

  def teardown
    @db.disconnect if @db
  end

  def test_connect
    assert @db
  end

  def test_disconnect
    @db.disconnect
    assert ! @db.connected?
    @db = nil
  end

  ## Make sure that we can prepare a SELECT statement and fetch data.
  def test_prepare
    st = @db.prepare("SELECT age, name FROM #{$cfg.tables[0]} ORDER BY age")
    assert st
    st.execute    

    row = st.fetch
    assert_equal 2, row.size
    assert_equal "Adam", row[1]
    assert_equal 20, row[0]
  ensure
    st.finish if st
  end

  ## Make sure do works
  def test_do
    @db.do("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES ('Zeb', 12)")
    check_insert
  ensure
    clear_insert
  end

  ## Make sure do with parameters works
  def test_do_with_parameters
    @db.do("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES (?,?)", 'Zeb', 12)
    check_insert
  ensure
    clear_insert
  end

  ## Make sure execute works
  def test_execute
    st = @db.prepare("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES ('Zeb', 12)")
    st.execute
    check_insert
  ensure
    clear_insert
    st.finish if st
  end

  ## Make sure execute works
  def test_execute_with_parameters
    st = @db.prepare("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES (?,?)")
    st.execute('Zeb', 12)
    check_insert
  ensure
    clear_insert
    st.finish if st
  end

  ## Make sure we can insert/read null data
  def test_insert_null
    @db.do("INSERT INTO #{$cfg.tables[0]} (name) VALUES ('Zeb')")
    st = @db.prepare("SELECT name, age FROM #{$cfg.tables[0]} WHERE name='Zeb'")
    st.execute
    row = st.fetch
    assert_equal "Zeb", row[0]
    assert_equal nil, row[1]
  ensure
    clear_insert
    st.finish if st
  end

  def test_ping
    assert @db.ping
  end

end

$last_suite.add_test(TestDbiDb.suite)


######################################################################
class TestDbiStatement < RUNIT::TestCase

  include DbiTestUtils

  def test_column_names
    db = get_db_auto
    st = db.prepare "SELECT name, age from #{$cfg.tables[0]}"
    st.execute
    info = st.column_info
    assert_equal 'name', info[0]['name']
    assert_equal 'age', info[1]['name']
    fields = st.column_names
    assert_equal 'name', fields[0]
    assert_equal 'age',  fields[1]
  ensure
    st.finish if st
    db.disconnect if db
  end

  ## Make sure that a fetch loop terminates.
  def test_fetch_loop
    db = get_db_auto
    expected_ages = [20,21,22]
    expected_names = ["Adam", "Bob", "Charlie"]
    st = db.prepare("SELECT age, name FROM #{$cfg.tables[0]} ORDER BY age")
    st.execute
    while row = st.fetch
      assert_equal expected_ages.shift,  row[0]
      assert_equal expected_names.shift, row[1]
    end
  ensure
    st.finish if st
    db.disconnect if db
  end
  
  def test_block_fetch_with_parameters
    db = get_db_auto
    st = db.prepare("SELECT age, name FROM #{$cfg.tables[0]} " +
		    "WHERE age=? " +
		    "ORDER BY age")
    st.execute(21)
    count = 0
    st.fetch { |row|
      assert_equal 21,  row[0]
      assert_equal "Bob", row[1]
      count += 1
    }
    assert_equal 1, count, "Too many rows retrieved"
  ensure
    st.finish if st
    db.disconnect if db
  end

  def test_block_fetch
    db = get_db_auto
    expected_ages = [20,21,22]
    expected_names = ["Adam", "Bob", "Charlie"]
    st = db.prepare("SELECT age, name FROM #{$cfg.tables[0]} ORDER BY age")
    st.execute
    st.fetch { |row|
      assert_equal expected_ages.shift,  row[0]
      assert_equal expected_names.shift, row[1]
    }
  ensure
    st.finish if st
    db.disconnect if db
  end

  def test_bind_param_bug
    db = get_db_auto
    st = db.prepare("SELECT age, name FROM #{$cfg.tables[0]} " +
		    "WHERE age=? ")

    st.bind_param(1, 21)
    st.execute
  ensure
    st.finish if st
    db.disconnect if db
  end


end

$last_suite.add_test(TestDbiStatement.suite)


######################################################################
class TestDbiPostgresErrors < RUNIT::TestCase

  include DbiTestUtils
  
  def test_bad_db_name
    db = nil
    assert_exception(DBI::OperationalError) {
      db = DBI.connect "DBI:#{$cfg.driver}:bad_database_name", $cfg.user
    }
  ensure
    db.disconnect if db
  end

  def test_missing_user
    db = nil
    assert_exception(DBI::OperationalError) {
      db = DBI.connect "DBI:#{$cfg.driver}:bad_database_name"
    }
  ensure
    db.disconnect if db
  end

  def test_bad_sql
    db = get_db_auto
    st = nil
    assert_exception(DBI::ProgrammingError) {
      st = db.prepare("INSRT into xyz")
      st.execute
    }
  ensure
    st.finish if st
    db.disconnect if db
  end
  
  def test_too_many_parameters
    db = get_db_auto
    st = nil
    assert_exception(DBI::ProgrammingError) {
      st = db.prepare("SELECT name, age FROM #{$cfg.tables[0]} WHERE age=?")
      st.execute(10, 11)
    }
  ensure
    st.finish if st
    db.disconnect if db
  end

  def test_few_many_parameters
    db = get_db_auto
    st = nil
    assert_exception(DBI::ProgrammingError) {
      st = db.prepare("SELECT name, age FROM #{$cfg.tables[0]} WHERE age=? AND name=?")
      st.execute(10)
    }
  ensure
    st.finish if st
    db.disconnect if db
  end

end

$last_suite.add_test(TestDbiPostgresErrors.suite)



######################################################################
class TestDbiTransactions < RUNIT::TestCase

  include DbiTestUtils
  include DbiInsertUtils

  def setup
    @db = get_db_tran
  end

  def teardown
    @db.disconnect
  end

  ## Make sure we can commit
  def test_commit
    @db.do("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES ('Zeb', 12)")
    @db.commit
    check_insert
  ensure
    clear_insert
  end

  ## Make sure we can rollback
  def test_rollback
    @db.do("INSERT INTO #{$cfg.tables[0]} (name, age) VALUES ('Zeb', 12)")
    @db.rollback
    check_no_insert
  ensure
    clear_insert
  end
end

$last_suite.add_test(TestDbiTransactions.suite)


######################################################################
class TestDbiQuoting < RUNIT::TestCase

  include DbiTestUtils

  ## Make sure we can properly quote both strings and other types.
  def test_quoting
    db = get_db_auto
    assert_equal %q(''),       db.quote("")
    assert_equal %q('hi'),     db.quote("hi")
    assert_equal %q('g''day'), db.quote("g'day")
    assert_equal(%q('~`!@#$%^&*-_=+''()[]{}";:<>,.?/'),
                  db.quote('~`!@#$%^&*-_=+\'()[]{}";:<>,.?/'))

    assert_equal %q(123),      db.quote(123)
    assert_equal %q(3.1416),   db.quote(3.1416)

    assert_equal %q(NULL),     db.quote(nil)
  ensure
    db.disconnect if db
  end

end

$last_suite.add_test(TestDbiQuoting.suite)


######################################################################
# Wrap the previous tests in a test that initializes the database by
# running the configured setup and teardown scripts.
#
class DbiWrapper < RUNIT::TestCase

  include DbiTestUtils

  def initialize(suite)
    @test_suite = suite
  end

  def setup
    system "#{$cfg.setup} >test.log 2>&1"
  end

  def teardown
    system "#{$cfg.teardown} >>test.log 2>&1"
  end

  def run(test_result)
    setup
    @test_suite.run(test_result)
    teardown
  end
end

$last_suite = DbiWrapper.new($last_suite)


######################################################################
# Wrap the previous tests in a test that will run the given tests
# through a series of different configurations.
#
class DbiConfigBuilder < RUNIT::TestCase

  include DbiTestUtils

  def initialize(suite)
    @test_suite = suite
  end

  def run(test_result)
    Dir['*.cfg'].each { |fn|
      puts "\nRunning with Config File #{fn}"
      $cfg = DbiTestConfig.load(fn)
      @test_suite.run(test_result)
    }
  end
end

$last_suite = DbiConfigBuilder.new($last_suite)

# --------------------------------------------------------------------

if __FILE__ == $0 then
  RUNIT::CUI::TestRunner.quiet_mode = true
  RUNIT::CUI::TestRunner.run($last_suite)
end
