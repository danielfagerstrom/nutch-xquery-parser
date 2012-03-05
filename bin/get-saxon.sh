#! /bin/sh
cd /tmp
wget http://sourceforge.net/projects/saxon/files/Saxon-HE/9.4/SaxonHE9-4-0-1J.zip/download
unzip download
mvn install:install-file -Dfile=saxon9he.jar -DgroupId=net.sf.saxon -DartifactId=saxon-he -Dversion=9.3 -Dpackaging=jar
