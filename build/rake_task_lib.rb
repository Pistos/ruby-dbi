$:.unshift 'lib'
require 'rake'
require 'rake/gempackagetask'
require 'rake/packagetask'
require 'rake/rdoctask'

DEFAULT_TASKS = [:clobber_package, :package, :gem]

DBD_GEM_DEP_MAP = {
    'pg'      => 'pg',
    'mysql'   => 'mysql',
    'sqlite'  => 'sqlite-ruby',
    'sqlite3' => 'sqlite3-ruby'
}

#
# Packaging
#

PACKAGE_FILES = %w(Rakefile build/rake_task_lib.rb setup.rb)
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

        package_files(code_files).each do |x|
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

def dbd_gem_spec(dbd, dbd_const, code_files)
    spec = boilerplate_spec
    spec.name        = dbd_namespace(dbd)
    spec.version     = dbd_version(dbd_const)
    spec.test_file   = 'test/ts_dbd.rb'
    spec.files       = gem_files(code_files) 
    spec.summary     = dbd_description(dbd_const)
    spec.description = dbd_description(dbd_const) 
    spec.add_dependency 'dbi', '>= 0.4.0'

    dcdbd = dbd.downcase

    if DBD_GEM_DEP_MAP[dcdbd]
        spec.add_dependency DBD_GEM_DEP_MAP[dcdbd]
    end

    return spec
end

def dbd_version(const)
    DBI::DBD.const_get(const).const_get("VERSION")
end

def dbd_description(const)
    DBI::DBD.const_get(const).const_get("DESCRIPTION")
end


def build_dbd_tasks(dbd)
    task :default => DEFAULT_TASKS

    begin
        done = false
        dbd_const = nil
        Dir["lib/dbd/*.rb"].each do |dbd_file|
            if File.basename(dbd_file.downcase, '.rb') == dbd.to_s.downcase
                dbd_const = File.basename(dbd_file, '.rb')
                require "dbd/#{dbd_const}"
                done = true
            end
        end

        abort "No DBD found even though we asked to make tasks for it" unless done

        code_files = dbd_code_files(dbd_const) 

        spec = dbd_gem_spec(dbd, dbd_const, code_files)

        build_package_tasks(spec, code_files)

        # FIXME: convert to a rake_test_loader sooner or later
        task :test do
            ENV["DBTYPES"] = dbd
            ruby "test/ts_dbd.rb"
        end
    rescue LoadError => e
        DEFAULT_TASKS.each do |x|
            task x do
            end
        end
        warn "Skipping #{dbd_namespace(dbd)} because we can't require DBD"
    end
end

def build_dbi_tasks
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
    rd.options = %w(-apMN)
end

