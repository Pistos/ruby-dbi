$:.unshift 'lib'
require 'dbi'
require 'rake'
require 'rake/gempackagetask'
require 'rake/packagetask'
require 'rake/rdoctask'

DBD_PACKAGES = {
    'sqlite'   => ['SQLite', 'SQLite 2.x DBD for Ruby/DBI'],
    'sqlite3'  => ['SQLite3', 'SQLite 3.x DBD for Ruby/DBI'],
    'pg'       => ['Pg', 'PostgreSQL DBD for Ruby/DBI'],
    'mysql'    => ['Mysql', 'MySQL DBD for Ruby/DBI'],
}

DEFAULT_TASKS = [:clobber_package, :package, :gem]

#
# some inlines
#

# creates a number of tasks like dbi:task_name, dbd_mysql:task_name, so on.
# Builds these out into an array that can be used as a prereq for other tasks.
def map_task(task_name)
    namespaces = (['dbi'] + DBD_PACKAGES.keys.collect { |x| dbd_namespace(x) }).flatten
    namespaces.collect { |x| [x, task_name].join(":") }
end

# builds a dbd namespace from the DBD_PACKAGES hash
def dbd_namespace(shortname)
    "dbd-" + shortname.to_s
end

def gem_files(code_files)
    (code_files + DOC_FILES).collect { |x| Dir[x] }.reject { |x| EXCLUSIONS.include? x }.flatten
end


#
# basic tasks
#

task :dist      => [:distclean, :package, :rdoc]
task :distclean => [:clobber_package, :clobber_rdoc]
task :clean     => [:distclean]
task :default   => [:test, :dist]

task :package         => (map_task("package") + map_task("gem"))
task :clobber_package => map_task("clobber_package")

#
# Documentation
#

Rake::RDocTask.new do |rd|
    rd.rdoc_dir = "rdoc"
    rd.rdoc_files.include("./README")
    rd.rdoc_files.include("./ChangeLog")
    rd.rdoc_files.include("./LICENSE")
    rd.rdoc_files.include("./doc/**/*.rdoc")
    rd.rdoc_files.include("./lib/**/*.rb")
    rd.rdoc_files.include("./ext/**/*.c")
    rd.options = %w(-ap)
end

# Runs the DBI test suite (though not the various DBD tests)
# FIXME: convert to a rake_test_loader sooner or later
task :test do
    Dir["test/ts_*.rb"].each do |file|
        system("ruby", file)
    end
end

#
# Packaging
#

DOC_FILES  = %w(README LICENSE ChangeLog)
EXCLUSIONS = %w(test/sql.log)

#
# There's probably a better way to do this, but here's a boilerplate spec that we dup and modify.
#

gem = Gem::Specification.new 
gem.authors     = ['Erik Hollensbe', 'Christopher Maujean']
gem.email       = 'ruby-dbi-users@rubyforge.org'
gem.homepage    = 'http://www.rubyforge.org/projects/ruby-dbi'
gem.platform    = Gem::Platform::RUBY
gem.has_rdoc    = true
gem.extra_rdoc_files = DOC_FILES
gem.required_ruby_version = '>= 1.8.0'
gem.rubyforge_project = 'ruby-dbi'

task :dbi => DEFAULT_TASKS.collect { |x| "dbi:#{x.to_s}" }

namespace :dbi do
    code_files = %w(examples/**/* bin/**/* Rakefile lib/dbi.rb lib/dbi/* test/ts_dbi.rb test/dbi/*)

    spec = gem.dup
    spec.name        = 'dbi'
    spec.version     = DBI::VERSION
    spec.test_file   = 'test/ts_dbi.rb'
    spec.files       = gem_files(code_files)
    spec.summary     = 'A vendor independent interface for accessing databases, similar to Perl\'s DBI'
    spec.description = 'A vendor independent interface for accessing databases, similar to Perl\'s DBI'

    Rake::GemPackageTask.new(spec) do |s|
    end

    Rake::PackageTask.new(spec.name, spec.version) do |p|
        p.need_tar_gz = true
        p.need_zip = true

        (code_files + DOC_FILES).each do |x|
            p.package_files.include(x)
        end

        EXCLUSIONS.each do |x|
            p.package_files.exclude(x)
        end
    end
end

DBD_PACKAGES.each_key do |dbd|
    my_namespace = dbd_namespace(dbd)

    task my_namespace => DEFAULT_TASKS.collect { |x| "#{my_namespace}:#{x.to_s}" }
    namespace my_namespace do

        require "dbd/#{DBD_PACKAGES[dbd][0]}"

        task :default => DEFAULT_TASKS

        code_files = [
            "test/dbd/general/**", 
            File.join("test", "dbd", DBD_PACKAGES[dbd][0] == "pg" ? "postgresql" : DBD_PACKAGES[dbd][0], "*"), 
            File.join("lib", "dbd", DBD_PACKAGES[dbd][0] + ".rb"), 
            File.join("lib", "dbd", DBD_PACKAGES[dbd][0], "*")
        ]

        spec = gem.dup
        spec.name        = my_namespace
        spec.version     = DBI::DBD.const_get(DBD_PACKAGES[dbd][0]).const_get("VERSION")
        spec.test_file   = 'test/ts_dbd.rb'
        spec.files       = gem_files(code_files) 
        spec.summary     = DBD_PACKAGES[dbd][1] 
        spec.description = DBD_PACKAGES[dbd][1]

        Rake::GemPackageTask.new(spec) do |s|
        end

        Rake::PackageTask.new(spec.name, spec.version) do |p|
            p.need_tar_gz = true
            p.need_zip = true

            (code_files + DOC_FILES).each do |x|
                p.package_files.include(x)
            end

            EXCLUSIONS.each do |x|
                p.package_files.exclude(x)
            end
        end
    end
end
