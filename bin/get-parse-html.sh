#! /bin/sh
cp ~/pkg/nutch-1.4/runtime/local/plugins/parse-html/parse-html.jar /tmp/parse-html-1.4.jar
mvn install:install-file -Dfile=/tmp/parse-html-1.4.jar -DgroupId=org.apache.nutch -DartifactId=parse-html -Dversion=1.4 -Dpackaging=jar
