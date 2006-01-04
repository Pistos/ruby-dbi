require 'rake'

file 'doc/html/ToDo.html' => [ 'doc/ToDo' ] do
	sh "rd2 doc/ToDo > doc/html/ToDo.html"
end

task :docs => ['doc/html/ToDo.html']

task :test_dbi do
	sh "ruby test/dbi/all_tests.rb"
end

task :default => :test_dbi
