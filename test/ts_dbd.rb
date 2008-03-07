# figure out what tests to run
require 'yaml'
require 'test/unit'

module Test::Unit::Assertions
    def build_message(head, template=nil, *arguments)
        template += "\n" + "DATABASE: " + dbtype
        template &&= template.chomp
        #head = DBDConfig.current_dbtype.to_s + "\n" + (head || "")
        return AssertionMessage.new(head, template, arguments)
    end
end

module DBDConfig
    @testbase = { }
    @current_dbtype = nil

    def self.get_config
        config = nil

        begin
            config = YAML.load_file(File.join(ENV["HOME"], ".ruby-dbi.test-config.yaml"))
        rescue Exception => e
        end

        return config
    end

    def self.current_dbtype
        @current_dbtype
    end

    def self.current_dbtype=(setting)
        @current_dbtype = setting
    end

    def self.testbase(klass_name)
        return @testbase[klass_name]
    end

    def self.set_testbase(klass_name, klass)
        @testbase[klass_name] = klass
    end

    def self.suite
        @suite ||= []
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
            # base.rb is special, see DBD_TESTS
            require "dbd/#{dbtype}/base.rb"
            Dir["dbd/#{dbtype}/test_*.rb"].each { |file| require file }
            # run the general tests
            DBDConfig.current_dbtype = dbtype.to_sym
            Dir["dbd/general/test_*.rb"].each { |file| load file; DBDConfig.suite << @class }
        end
    end
end
