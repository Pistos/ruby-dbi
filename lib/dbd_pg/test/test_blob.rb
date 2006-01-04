#$Id: test_blob.rb,v 1.1 2006/01/04 02:03:21 francis Exp $

require "dbi"

DATA = "this is my new binary object"

DBI.connect("dbi:Pg:michael", "michael", "michael") do |dbh|
  begin
    dbh.do("DROP TABLE blob_test") 
  rescue; end

  dbh.do("CREATE TABLE blob_test (name VARCHAR(30), data OID)")

  dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)",
    "test", DBI::Binary.new(DATA))

  blob = dbh.func(:blob_create, PGlarge::INV_WRITE)
  blob.open
  blob.write DATA
  
  dbh.do("INSERT INTO blob_test (name, data) VALUES (?,?)",
    "test (2)", blob.oid)
  blob.close

  dbh.select_all("SELECT name, data FROM blob_test") do |name, data|
    print name, ": "

    # (1)
    if dbh.func(:blob_read, data) == DATA
      print "ok, "
    else
      print "wrong, "
    end

    # (2)
    dbh.func(:blob_export, data, '/tmp/dbitest')
    if File.readlines('/tmp/dbitest').to_s == DATA
      print "ok, "
    else
      print "wrong, "
    end

    # (3)
    blob = dbh.func(:blob_open, data, PGlarge::INV_READ)  
    blob.open
    if blob.read == DATA
      puts "ok"
    else
      puts "wrong"
    end
    blob.close

  end

end

puts "Test succeeded"

