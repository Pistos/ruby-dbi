require 'dbd/Pg'

begin
    require 'rubygems'
    gem 'pg'
rescue Exception => e
end

require 'pg'

LEN = 50

class TestPostgresByteA < DBDConfig.testbase(:postgresql)
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
        config = DBDConfig.get_config['postgresql']
        encoder = DBI::DBD::Pg::Database.new(config['dbname'], config['username'], config['password'], {})
        decoder = DBI::DBD::Pg::PgCoerce.new
        
        # some specific cases that were failing intermittenly
        # poor \\ handling
        str = "\236\000\257\202G<\371\035TPEO\211\005*AH'H\3136\360\004\245\261\037\340u\003s\\772X\231\002\200\n\327\202\217\353\177r\317o\341\237\341"
        encoded = encoder.__encode_bytea(str.dup)
        decoded = decoder.as_bytea(encoded)

        assert_equal str, decoded 

        # the split hack not working
        str = "\343\336e\260\337\373\314\026\323#\237i\035\0302\024\346X\274\016\324\371\206\036\230\374\206#rA\n\214\272\316\330\025\374\000\2663\244M\255x\360\002\266q\336\231"

        encoded = encoder.__encode_bytea(str.dup)
        decoded = decoder.as_bytea(encoded)

        assert_equal str, decoded 

        # delimiter at the end
        str = "\343\336e\260\337\373\314\026\323#\237i\035\0302\024\346X\274\016\324\371\206\036\230\374\206#rA\n\214\272\316\330\025\374\000\2663\244M\255x\360\002\266q\336\231\\\\\\\\"

        encoded = encoder.__encode_bytea(str.dup)
        decoded = decoder.as_bytea(encoded)

        assert_equal str, decoded 

        # a huge test to weed out all the stragglers
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
