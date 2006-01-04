require "dbi"

dbh = DBI.connect("dbi:Oracle:oracle.neumann", "scott", "tiger")

dbh.do("DROP TABLE BLOB_T")
dbh.do("CREATE TABLE BLOB_T (name VARCHAR2(256), data LONG RAW)")

sth = dbh.prepare("INSERT INTO BLOB_T VALUES(:1, :2)")

Dir["*"].each {|fil|
  next unless FileTest.file? fil
  sth.execute(fil, DBI::Binary.new(File.readlines(fil).to_s))
}   

dbh.disconnect
               
