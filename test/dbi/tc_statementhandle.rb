$: << 'lib'
require 'test/unit'
require 'dbi'

class TC_DBI_StatementHandle < Test::Unit::TestCase
  def test_fetch
		mock_handle = 'any_object'
		def mock_handle.cancel; end
		def mock_handle.column_info; {}; end
		def mock_handle.fetch; nil; end
		sth = DBI::StatementHandle.new( mock_handle, true, false )
		10.times do
			assert_nil sth.fetch
		end
  end
end
