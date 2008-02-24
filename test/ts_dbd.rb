Dir.chdir("..") if File.basename(Dir.pwd) == "test"
$LOAD_PATH.unshift(File.join(Dir.pwd, "lib"))
Dir.chdir("test") rescue nil

require 'dbi'

# figure out what tests to run
require 'yaml'

config = nil

begin
    config = YAML.load_file(File.join(ENV["HOME"], ".ruby-dbi.test-config.yaml"))
rescue Exception => e
end

if config
    config["dbtypes"].each do |dbtype|
        Dir["dbd/#{dbtype}/*.rb"].collect { |file| require file }
    end
end
