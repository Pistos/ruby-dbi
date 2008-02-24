require 'test/unit'

require 'dbd/Pg'

begin
    require 'rubygems'
    gem 'pg'
rescue Exception => e
end

require 'pg'

LEN = 50

class TestPostgresByteA < Test::Unit::TestCase
    # FIXME the 'pg' module is broken and doesn't encode/decode properly.
    # this test should prove that the 'pg' module works so we can back out our
    # hacks.
    def skip_underlying_driver 
        str = generate_random_string

        encoded = PGconn.escape_bytea(str.dup)
        decoded = PGconn.unescape_bytea(encoded)

        assert_equal str, decoded
    end

    def test_encode_decode
        encoder = DBI::DBD::Pg::Database.new('rubytest', 'erikh', 'monkeys', {})
        decoder = DBI::DBD::Pg::PgCoerce.new

        50_000.times do 
            str = generate_random_string

            encoded = encoder.__encode_bytea(str.dup)
            decoded = decoder.as_bytea(encoded)

            assert_equal str, decoded
        end
    end

    def generate_random_string
        # random string test
        str = " " * LEN
        for i in 0...LEN
            str[i] = (rand * 256).to_i.chr
        end

        return str
    end
end
