#!/bin/sh

# works only for tags above 0.0.17

export CVS_RSH=ssh

rm -rf work/
mkdir work

dialog --yesno "Modified lib/dbi/version.rb?" 8 40
if [ $? != 0 ]; then
  dialog --msgbox "Exiting! Please modify lib/dbi/version.rb appropriately, before trying again." 8 40
  rm -rf work/
  exit 1
fi

dialog --yesno "Did you run test.rb in this directory (with both old and new Ruby versions; ruby test.rb ruby18/ruby test.rb ruby)?" 8 40
if [ $? != 0 ]; then
  dialog --msgbox "Exiting! Please run before trying again." 8 40
  rm -rf work/
  exit 1
fi

dialog --inputbox "Tagged repository (e.g. cvs tag dbi-0-0-17)? Enter tag (without preceeding 'dbi-') below or choose 'Cancel'" 12 40 "0-0-" 2> work/VERSION
if [ $? != 0 ]; then
  dialog --msgbox "Exiting! Please tag repository, before trying again." 8 40
  rm -rf work/
  exit 1
fi
VERSION=`cat work/VERSION`
DOT_VERSION=`sed -e 's/-/./g' work/VERSION`
TAG=dbi-${VERSION}

# checkout sources
cd work
cvs -z3 -d:ext:mneumann@rubyforge.org:/var/cvs/ruby-dbi co -r ${TAG} src
cd src

# make documentation and ChangeLog
cd build
make all       
cd ..

# remove all CVS directories
pwd
find . -name "CVS" -print | xargs rm -rf

# upload HTML pages and CSS
cd doc/html
for i in *.html *.css ;
do scp $i mneumann@rubyforge.org:/var/www/gforge-projects/ruby-dbi
done
cd ../..
scp ChangeLog mneumann@rubyforge.org:/var/www/gforge-projects/ruby-dbi

# create tar.gz
FILE=ruby-dbi-all-${DOT_VERSION}.tar.gz
cd ..
mv src ruby-dbi-all
tar -cvzf ${FILE} ruby-dbi-all

dialog --msgbox "Now log into RubyForge Admin page and make a release. Release is named like '0.0.17'; choose Any and source .gz." 8 40
#w3m http://rubyforge.org/account/login.php

dialog --msgbox "Finally, update the page at the RAA." 8 40
w3m "http://raa.ruby-lang.org/update.rhtml?name=ruby-dbi"

dialog --msgbox "Proceed to clean up the work/ directory." 8 40
# remove work
cd ..
rm -rf work
