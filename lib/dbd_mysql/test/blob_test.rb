require "dbi"

DATA = (0..255).collect{|n| n.chr}.join("")
SQL  = "INSERT INTO blob_test (name, data) VALUES (?, ?)"

DBI.connect("dbi:mysql:michael", "michael", "michael") do |dbh|
  dbh.do("DROP TABLE blob_test") rescue nil
  dbh.do("CREATE TABLE blob_test (name VARCHAR(30), data BLOB)")

  dbh.do(SQL, 'test1', DBI::Binary.new(DATA)) 
  dbh.do(SQL, 'test2', DATA) 


  dbh.prepare(SQL) do |sth|
    sth.execute('test3', DBI::Binary.new(DATA))
    sth.execute('test4', DATA)
  end

  dbh.select_all("SELECT name, data FROM blob_test") do |name, data|
    print name, ": "
    if data == DATA
      print "ok\n"
    else
      print "wrong\n"
    end
  end
end



