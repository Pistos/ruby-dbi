require "dbi"

DBI.connect("dbi:Pg:michael:127.0.0.1", "michael", "michael") do |dbh|
  p dbh.ping
  puts "Shut now Pg down"; $stdin.readline
  p dbh.ping
end

