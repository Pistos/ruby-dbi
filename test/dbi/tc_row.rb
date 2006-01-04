$: << 'lib'
require 'test/unit'
require 'dbi/row'

# ====================================================================
class TestDbRow < Test::Unit::TestCase

  def test_create
    row = make_row
    assert_not_nil row
  end

  def test_size
    row = make_row
    assert_equal 3, row.length 
    assert_equal 3, row.size
  end

  def test_by_index
    row = make_row
    assert_equal sample_data[0], row.by_index(0)
    assert_equal sample_data[1], row.by_index(1)
    assert_equal sample_data[2], row.by_index(2)
    assert_nil row.by_index(3)
  end

  def test_by_field
    row = make_row
    assert_equal sample_data[0], row.by_field('first')
    assert_equal sample_data[1], row.by_field('last')
    assert_equal sample_data[2], row.by_field('age')
    assert_equal nil, row.by_field('unknown')
  end

  def test_indexing
    row = make_row
    assert_equal sample_data[0], row[0]
    assert_equal sample_data[0], row['first']
    assert_equal sample_data[1], row[1]
    assert_equal sample_data[1], row['last']
    assert_equal sample_data[2], row[2]
    assert_equal sample_data[2], row['age']
    assert_equal nil, row['unknown']
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

  def test_redefining_values
    row = make_row
    row.set_values(["John", "Doe", 23])
    assert_equal "John", row.by_index(0)
    assert_equal "Doe", row.by_index(1)
    assert_equal 23, row.by_index(2)
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

