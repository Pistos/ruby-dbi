$:.unshift 'build'

if File.exists? 'lib/dbi'
    require 'Rakefile.dbi'
elsif File.exists? 'lib/dbd'
    require 'Rakefile.dbd'
    build_dbd_tasks(File.basename(Dir['lib/dbd/*.rb'][0]).downcase.to_sym)
else
    abort "Well, this is odd; No DBI or DBD found."
end
