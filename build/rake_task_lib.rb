$:.unshift 'lib'
require 'rake'
require 'rake/gempackagetask'
require 'rake/packagetask'
require 'rake/rdoctask'

DEFAULT_TASKS = [:clobber_package, :package, :gem]

#
# Packaging
#

PACKAGE_FILES = %w(setup.rb)
DOC_FILES  = %w(README LICENSE ChangeLog)
EXCLUSIONS = %w(test/sql.log)
DBD_FILES  = %w(test/DBD_TESTS)

#
# some inlines
#

def gem_files(code_files)
    (code_files + DOC_FILES).collect { |x| Dir[x] }.reject { |x| EXCLUSIONS.include? x }.flatten
end

def package_files(code_files)
    code_files + DOC_FILES + PACKAGE_FILES
end

def build_package_tasks(spec, code_files)
    Rake::GemPackageTask.new(spec) do |s|
    end

    Rake::PackageTask.new(spec.name, spec.version) do |p|
        p.need_tar_gz = true
        p.need_zip = true

        code_files.each do |x|
            p.package_files.include(x)
        end

        EXCLUSIONS.each do |x|
            p.package_files.exclude(x)
        end
    end
end

def boilerplate_spec
    gem = Gem::Specification.new 
    gem.authors     = ['Erik Hollensbe', 'Christopher Maujean']
    gem.email       = 'ruby-dbi-users@rubyforge.org'
    gem.homepage    = 'http://www.rubyforge.org/projects/ruby-dbi'
    gem.platform    = Gem::Platform::RUBY
    gem.has_rdoc    = true
    gem.extra_rdoc_files = DOC_FILES
    gem.required_ruby_version = '>= 1.8.0'
    gem.rubyforge_project = 'ruby-dbi'
    return gem
end

# builds a dbd namespace from the DBD_PACKAGES hash
def dbd_namespace(dbd)
    "dbd-" + dbd.to_s.downcase
end

def dbd_code_files(dbd)
    code_files = [
                "test/dbd/general/**", 
                File.join("test", "dbd", dbd.downcase == "pg" ? "postgresql" : dbd.downcase, "*"), 
                File.join("lib", "dbd", dbd + ".rb"), 
                "lib/dbd/#{dbd.downcase}/*.rb",
    ] + DBD_FILES
end

def dbd_gem_files(code_files)
    DBD_FILES + gem_files(code_files)
end

def dbd_package_files(code_files)
    DBD_FILES + package_files(code_files)
end

def dbd_gem_spec(dbd, code_files)
    spec = boilerplate_spec
    spec.name        = dbd_namespace(dbd)
    spec.version     = dbd_version(dbd)
    spec.test_file   = 'test/ts_dbd.rb'
    spec.files       = gem_files(code_files) 
    spec.summary     = dbd_description(dbd)
    spec.description = dbd_description(dbd) 

    return spec
end

def dbd_version(dbd)
    DBI::DBD.const_get(dbd).const_get("VERSION")
end

def dbd_description(dbd)
    DBI::DBD.const_get(dbd).const_get("DESCRIPTION")
end

#
# basic tasks
#

task :dist      => [:distclean, :package, :rdoc]
task :distclean => [:clobber_package, :clobber_rdoc]
task :clean     => [:distclean]
task :default   => [:test, :dist]

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
