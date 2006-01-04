require 'dbi'
  
File.unlink("dbierrortestdb") rescue nil
db = DBI.connect("dbi:SQLite:dbierrortestdb")
db.execute("create table sequences (name varchar(30), val integer)")
db.execute("insert into sequences (name,val) values ('test',1000)")
  
puts "Before: #{db.select_all('select * from sequences').inspect}"
  
sth = db.prepare("update sequences set val=? where val=? and name=?")
sth.execute(1001,1000,"test")

rows = sth.rows

puts "Rows changed: #{rows}"

puts "After: #{db.select_all('select * from sequences').inspect}"

if rows != 1 
  puts "TEST FAILED"
  exit -1 
else
  puts "TEST PASSED"
  exit 0
end
