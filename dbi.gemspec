require 'rubygems'

spec = Gem::Specification.new do |gem|
   gem.name        = 'dbi'
   gem.version     = '0.2.0'
   gem.author      = 'Daniel J. Berger'
   gem.email       = 'djberg96@gmail.com'
   gem.homepage    = 'http://www.rubyforge.org/projects/ruby-dbi'
   gem.platform    = Gem::Platform::RUBY
   gem.summary     = 'A vendor independent interface for accessing databases'
   gem.description = 'A vendor independent interface for accessing databases'
   gem.test_file   = 'test/ts_dbi.rb'
   gem.has_rdoc    = true
   gem.files       = Dir['lib/*'] + Dir['lib/dbi/*'] + Dir['test/*']
   gem.files.reject! { |fn| fn.include? 'CVS' }
   gem.extra_rdoc_files = ['README', 'CHANGES', 'MANIFEST']
   gem.required_ruby_version = '>= 1.8.0'
   gem.rubyforge_project = 'ruby-dbi'
end

if $PROGRAM_NAME == __FILE__
   Gem.manage_gems
   Gem::Builder.new(spec).build
end