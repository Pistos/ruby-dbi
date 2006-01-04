# Demonstrates bug in ruby-dbi with SQLite
# Prepared statement is executed twice: once with no match, once with
# a match. The second case falls over.

require 'dbi'

File.unlink("dbierrortestdb") rescue nil
db = DBI.connect("dbi:SQLite:dbierrortestdb")
db.execute("create table foo (bar integer)")
db.execute("insert into foo (bar) values (99)")

sth = db.prepare("select * from foo where bar=?")

puts "First time:"
sth.execute(3)
p sth.fetch

puts "Second time:"
sth.execute(99)
p sth.fetch

# /usr/local/lib/ruby/site_ruby/1.6/dbi/dbi.rb:794: [BUG] Segmentation fault
# ruby 1.6.8 (2002-12-24) [i386-freebsd4.7]
# Abort trap (core dumped)
