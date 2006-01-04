all: ChangeLog.html doc

ChangeLog.html: ../ChangeLog
	./cl2html.sh

../ChangeLog:
	(cd .. && ./build/cvs2cl.pl --no-wrap --usermap build/USER --prune --tagdates --separate-header --summary --gmt) 

doc:
	./create_html

clean:
	rm -f ../ChangeLog ../doc/html/ChangeLog.html ../doc/html/ToDo.html ../doc/html/DBD_SPEC.html ../doc/html/DBI_SPEC.html ../doc/html/index.html
