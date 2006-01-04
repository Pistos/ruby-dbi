require "dbi"
require "../Pg"

class MyDB < DBI::DBD::Pg::Database
  def initialize; end
end

$encoder = MyDB.new
$decoder = DBI::DBD::Pg::PgCoerce.new

# random string test
LEN = 50
STR = " " * LEN

50_000.times do 
  for i in 0...LEN
    STR[i] = (rand * 256).to_i.chr
  end

  encoded = $encoder.__encode_bytea(STR.dup)
  decoded = $decoder.as_bytea(encoded)
 
  unless STR == decoded
    p STR
    puts "---------------"
    p encoded
    puts "---------------"
    p decoded
    raise "conversion failed!"
  end
end
