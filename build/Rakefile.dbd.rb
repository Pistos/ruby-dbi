# XXX This is a rakefile that is included with DBD gems and tarballs.
#     It should not be used for development; it's purpose is to aid end-users.
#     Use the Rakefile in the root directory for development.

$:.unshift 'lib'
require 'rake_task_lib'
require 'rake'
require 'rake/gempackagetask'
require 'rake/packagetask'
require 'rake/rdoctask'

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

PACKAGE_FILES = %w(setup.rb)
DOC_FILES  = %w(README LICENSE ChangeLog)
EXCLUSIONS = %w(test/sql.log)
DBD_FILES  = %w(test/DBD_TESTS)

def gem_files(code_files)
    (code_files + DOC_FILES).collect { |x| Dir[x] }.reject { |x| EXCLUSIONS.include? x }.flatten
end

code_files = [
    "test/dbd/general/**", 
    File.join("test", "dbd", '@DBD'.downcase == "pg" ? "postgresql" : '@DBD'.downcase, "*"), 
    File.join("lib", "dbd", '@DBD@' + ".rb"), 
    "lib/dbd/#{'@DBD@'.downcase}/*.rb",
    DBD_FILES
]

Rake::GemPackageTask.new(spec) do |s|
    spec = Gem::Specification.new 
    spec.authors     = ['Erik Hollensbe', 'Christopher Maujean']
    spec.email       = 'ruby-dbi-users@rubyforge.org'
    spec.homepage    = 'http://www.rubyforge.org/projects/ruby-dbi'
    spec.platform    = Gem::Platform::RUBY
    spec.has_rdoc    = true
    spec.extra_rdoc_files = DOC_FILES
    spec.required_ruby_version = '>= 1.8.0'
    spec.rubyforge_project = 'ruby-dbi'
    spec.name        = '@PACKAGE@'
    spec.version     = DBI::DBD.const_get('@DBD@').const_get("VERSION")
    spec.test_file   = 'test/ts_dbd.rb'
    spec.files       = gem_files(code_files + DOC_FILES) 
    spec.summary     = '@SUMMARY@'
    spec.description = '@DESCRIPTION@'

end

Rake::PackageTask.new(spec.name, spec.version) do |p|
    p.need_tar_gz = true
    p.need_zip = true

    (code_files + DOC_FILES + PACKAGE_FILES).each do |x|
        p.package_files.include(x)
    end

    EXCLUSIONS.each do |x|
        p.package_files.exclude(x)
    end
end
