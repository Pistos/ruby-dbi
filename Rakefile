require 'rake'

file 'doc/html/ToDo.html' => [ 'doc/ToDo' ] do
	sh "rd2 doc/ToDo > doc/html/ToDo.html"
end

task 'docs' => ['doc/html/ToDo.html']
