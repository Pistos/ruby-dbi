RUBY = ARGV[0] || "ruby"

# test syntax for all .rb files
`find .. -name "*.rb" -print`.each do |script|
  script.chomp!
  print script.ljust(50)
  print `#{ RUBY } -c #{ script }`
  if $? != 0
    puts "FAILURE"
    exit -1
  end
end

# execute tests in lib/dbi/test
puts "="*60
Dir.chdir("../lib/dbi/test")
Dir["test*.rb"].each do |test|
  puts test
  str = `#{ RUBY } #{ test }`
  puts str
  if str =~ /Failure/
    puts "FAILURE"
    exit -1
  end
  puts "-"*60
end
