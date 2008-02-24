# figure out what tests to run
require 'yaml'

module DBDConfig
    def self.get_config
        config = nil

        begin
            config = YAML.load_file(File.join(ENV["HOME"], ".ruby-dbi.test-config.yaml"))
        rescue Exception => e
        end

        return config
    end
end

if __FILE__ == $0
    Dir.chdir("..") if File.basename(Dir.pwd) == "test"
    $LOAD_PATH.unshift(File.join(Dir.pwd, "lib"))
    Dir.chdir("test") rescue nil

    require 'dbi'

    config = DBDConfig.get_config

    if config
        config["dbtypes"].each do |dbtype|
            Dir["dbd/#{dbtype}/*.rb"].collect { |file| require file }
        end
    end
end
