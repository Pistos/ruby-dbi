require 'rake_task_lib'
require 'dbi'

DBD_PACKAGES = Dir['lib/dbd/*.rb'].collect { |x| File.basename(x, '.rb') }

# creates a number of tasks like dbi:task_name, dbd_mysql:task_name, so on.
# Builds these out into an array that can be used as a prereq for other tasks.
def map_task(task_name)
    namespaces = (['dbi'] + DBD_PACKAGES.collect { |x| dbd_namespace(x) }).flatten
    namespaces.collect { |x| [x, task_name].join(":") }
end

task :package         => (map_task("package") + map_task("gem"))
task :clobber_package => map_task("clobber_package")

#
# There's probably a better way to do this, but here's a boilerplate spec that we dup and modify.
#

task :dbi => DEFAULT_TASKS.collect { |x| "dbi:#{x.to_s}" }

namespace :dbi do
    code_files = %w(examples/**/* bin/**/* Rakefile lib/dbi.rb lib/dbi/* test/ts_dbi.rb test/dbi/*)

    spec = boilerplate_spec
    spec.name        = 'dbi'
    spec.version     = DBI::VERSION
    spec.test_file   = 'test/ts_dbi.rb'
    spec.files       = gem_files(code_files)
    spec.summary     = 'A vendor independent interface for accessing databases, similar to Perl\'s DBI'
    spec.description = 'A vendor independent interface for accessing databases, similar to Perl\'s DBI'

    build_package_tasks(spec, code_files)
end

DBD_PACKAGES.each do |dbd|
    my_namespace = dbd_namespace(dbd)

    task my_namespace => DEFAULT_TASKS.collect { |x| "#{my_namespace}:#{x.to_s}" }
    namespace my_namespace do
        task :default => DEFAULT_TASKS

        begin
            require "dbd/#{dbd}"

            code_files = dbd_code_files(dbd) 

            spec = dbd_gem_spec(dbd, code_files)

            build_package_tasks(spec, code_files)
        rescue LoadError => e
            DEFAULT_TASKS.each do |x|
                task x do
                end
            end
            warn "Skipping #{my_namespace} because we can't require DBD"
        end
    end
end
