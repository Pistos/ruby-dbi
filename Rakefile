$:.unshift 'lib'
require 'dbi'
require 'rake'
require 'rake/gempackagetask'
require 'rake/packagetask'
require 'rake/rdoctask'

#
# basic tasks
#

task :dist      => [:repackage, :gem, :rdoc]
task :distclean => [:clobber_package, :clobber_rdoc]
task :clean     => [:distclean]
task :default => [ :test, :dist ]

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

# gemspec
spec = Gem::Specification.new do |gem|
    gem.name        = 'dbi'
    gem.version     = DBI::VERSION
    gem.authors     = ['Erik Hollensbe', 'Christopher Maujean']
    gem.email       = 'ruby-dbi-users@rubyforge.org'
    gem.homepage    = 'http://www.rubyforge.org/projects/ruby-dbi'
    gem.platform    = Gem::Platform::RUBY
    gem.summary     = 'A vendor independent interface for accessing databases'
    gem.description = 'A vendor independent interface for accessing databases'
    gem.test_file   = 'test/ts_dbi.rb'
    gem.has_rdoc    = true
    gem.extensions  += [ 'ext/dbd_sqlite/extconf.rb', 'ext/dbd_sybase/extconf.rb' ]
    gem.files       = Dir['examples/**/*'] + Dir['ext/**/*'] + Dir['lib/**/*'] + Dir['test/*'] + Dir['README'] + Dir['LICENSE'] + Dir['ChangeLog']
    gem.extra_rdoc_files = ['./README']
    gem.required_ruby_version = '>= 1.8.0'
    gem.rubyforge_project = 'ruby-dbi'
end

#
# Packaging
#

Rake::GemPackageTask.new(spec) do |s|
end

Rake::PackageTask.new(spec.name, spec.version) do |p|
    p.need_tar_gz = true
    p.need_zip = true
    p.package_files.include("./examples/**/*")
    p.package_files.include("./bin/**/*")
    p.package_files.include("./Rakefile")
    p.package_files.include("./setup.rb")
    p.package_files.include("./lib/**/*")
    p.package_files.include("./ext/**/*")
    p.package_files.include("./test/**/*")
    p.package_files.include("./README")
    p.package_files.include("./LICENSE")
    p.package_files.include("./ChangeLog")
end
