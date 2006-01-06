######################################################################
# tc_row.rb
#
# Test case for the DBI::Row class. 
######################################################################
$LOAD_PATH.unshift(Dir.pwd)
$LOAD_PATH.unshift(File.dirname(Dir.pwd))
$LOAD_PATH.unshift("../../lib")
$LOAD_PATH.unshift("../../lib/dbi")

require 'test/unit'
require 'dbi/row'

class TC_DBD_Row < Test::Unit::TestCase
   def setup
      @data = %w/Daniel Berger 36/
      @cols = %w/first last age/
      @row  = DBI::Row.new(@cols, @data.clone)
   end

   def teardown
      @data = nil
      @cols = nil
      @row  = nil
   end
   
   # Ensure that constructor only allows Integers or Arrays (or nil)
   def test_row_constructor
      assert_nothing_raised{ DBI::Row.new(@cols) }
      assert_nothing_raised{ DBI::Row.new(@cols, 1) }
      assert_nothing_raised{ DBI::Row.new(@cols, nil) }
      assert_nothing_raised{ DBI::Row.new(@cols, [1,2,3])}
      assert_raises(ArgumentError){ DBI::Row.new }
      assert_raises(TypeError){ DBI::Row.new(@cols, {}) }
   end
   
   # Should respond to Array and Enumerable methods
   def test_row_delegate
      assert_respond_to(@row, :length)
      assert_respond_to(@row, :each)
      assert_respond_to(@row, :grep)
   end
   
   def test_row_length
      assert_equal(3, @row.length)
      assert_equal(3, DBI::Row.new(@cols).length)
   end
   
   def test_row_data_by_index
      assert_equal(@data[0], @row.by_index(0))
      assert_equal(@data[1], @row.by_index(1))
      assert_equal(@data[2], @row.by_index(2))
      assert_nil(@row.by_index(3))
   end
   
   def test_row_data_by_field
      row = make_row
      assert_equal sample_data[0], row.by_field('first')
      assert_equal sample_data[1], row.by_field('last')
      assert_equal sample_data[2], row.by_field('age')
      assert_equal nil, row.by_field('unknown')
   end
   
   def test_row_set_values
      assert_respond_to(@row, :set_values)
      assert_nothing_raised{ @row.set_values(["John", "Doe", 23]) }
      assert_equal("John", @row.by_index(0))
      assert_equal("Doe", @row.by_index(1))
      assert_equal(23, @row.by_index(2))
   end
   
   def test_row_to_h
      assert_respond_to(@row, :to_h)
      assert_nothing_raised{ @row.to_h }
      assert_kind_of(Hash, @row.to_h)
      assert_equal({"first"=>"Daniel", "last"=>"Berger", "age"=>"36"},@row.to_h)
   end
   
   def test_row_column_names
      assert_respond_to(@row, :column_names)
      assert_nothing_raised{ @row.column_names }
      assert_kind_of(Array, @row.column_names)
      assert_equal(["first", "last", "age"], @row.column_names)
   end
   
   def test_indexing_numeric
      assert_equal(@data[0], @row[0])   
      assert_equal(@data[1], @row[1])  
      assert_equal(@data[2], @row[2])      
   end
   
   def test_indexing_string_or_symbol
      assert_equal(@data[0], @row['first'])
      assert_equal(@data[0], @row[:first])
      assert_equal(@data[1], @row['last'])
      assert_equal(@data[2], @row['age'])
      assert_equal(nil, @row['unknown'])
   end
   
   def test_indexing_regexp
      assert_equal(["Daniel"], @row[/first/])
      assert_equal(["Berger"], @row[/last/])
      assert_equal(["36"], @row[/age/])
      assert_equal(["Daniel", "Berger"], @row[/first|last/])
      assert_equal([], @row[/bogus/])
   end

   def test_indexing_array
      assert_equal(["Daniel"], @row[[0]])
      assert_equal(["Daniel"], @row[["first"]])
      assert_equal(["Berger"], @row[[1]])
      assert_equal(["36"], @row[[2]])
      assert_equal([nil], @row[[3]])
      assert_equal(["Daniel", "36"], @row[[0,2]])
      assert_equal(["Daniel", "36"], @row[[0,:age]])
   end

   def test_indexing_range
      assert_equal(["Daniel","Berger"], @row[0..1])
      assert_equal(["Berger","36"], @row[1..2])
      assert_equal(["Berger","36"], @row[1..99])
      assert_equal(nil, @row[90..100])
   end

   # The two argument reference should behave like the second form of Array#[]
   def test_indexing_two_args
      assert_equal([], @row[0,0])
      assert_equal(["Daniel"], @row[0,1])
      assert_equal(["Daniel", "Berger"], @row[0,2])
      assert_equal(["Daniel", "Berger", "36"], @row[0,3])
      assert_equal(["Daniel", "Berger", "36"], @row[0,99])
   end

   def test_indexing_multiple_args
      assert_equal(["Berger", "36", "Daniel"], @row[:last, :age, :first])
      assert_equal(["Berger", "36", "Daniel"], @row[1, :age, :first])
      assert_equal(["Berger", "36", "Daniel"], @row[1, 2, :first])
      assert_equal(["Berger", "36", "Daniel"], @row[1, 2, 0])
      assert_equal(["Berger", "36", "Daniel", nil], @row[1, 2, 0, 9])
      assert_equal(["Berger", "36", "Daniel", nil], @row[1, 2, 0, :bogus])
   end

  def test_create
    row = make_row
    assert_not_nil row
  end

  def test_iteration
    row = make_row
    expect = sample_data.clone
    row.each { |value|
      assert_equal expect.shift, value
    }
    assert_equal [], expect
    row.collect { |value| "Field=#{value}" }
  end

  

  def test_clone_with
    row = make_row
    another_row = row.clone_with(["Jane", "Smith", 33])
    assert_equal "Jane", another_row.by_index(0)
    assert_equal "Smith", another_row.by_index(1)
    assert_equal 33, another_row.by_index(2)
    assert row != another_row
  end

  def test_to_array
    assert_equal sample_data, make_row.to_a
  end

  def test_dup_clone
    row = make_row
    dupped = row.dup
    cloned = row.clone
    row.set_values(["Bill", "Jones", 16])
    assert_equal sample_data, dupped.to_a
    assert_equal sample_data, cloned.to_a
  end

  def test_dup_ruby18
    res = []
    r = DBI::Row.new(["col1","col2"],[nil,nil])

    [["one",1],["two",2],["three",3]].each do |x,y|
      r["col1"] = x
      r["col2"] = y
      res << r.dup
    end

    assert_equal res, [["one", 1], ["two", 2], ["three", 3]]
  end

  private

  def make_row
    names  = %w(first last age)
    DBI::Row.new(names, sample_data.clone)
  end

  def sample_data
    ['Jim', 'Weirich', 45]
  end

end

