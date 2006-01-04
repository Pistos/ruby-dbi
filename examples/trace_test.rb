require "dbi"
require "dbi/trace"

DBI.trace(0)

dbh = DBI.connect('dbi:Mysql:database=test')

dbh.do('CREATE TABLE trace_test (name VARCHAR(30))')

dbh.trace(1, File.new('trace.log', 'w+'))

sql = 'INSERT INTO trace_test VALUES (?)'
dbh.prepare(sql) do |sth|
  sth.execute('Michael')

  sth.trace(2)

  sth.execute('John')
end

dbh.do('DROP TABLE trace_test')

dbh.select_one('SELECT * FROM trace_test')

dbh.disconnect
