Nutch XQuery Parser
===================

Parses the html content in segment directories from Apache nutch using XQuery scripts.
What XQuery script that is used for the parsing is based on:

* Domain name
* regexp on the URL path
* XPath on the document (optionally)

This is configured in an XML file.

Build
-----

Download Nutch 1.4

	bin/get-parse-html.sh
	
	bin/get-saxon.sh
	
	mvn install
	
To create an eclipse project:
	
	mvn eclipse:eclipse -DdownloadJavadocs=true -DdownloadSources=true
	
To create a runable jar

	mvn package
	
Execute
-------

On a segment directory:

	java -jar target/nutch-xquery-parser-1.0-SNAPSHOT-jar-with-dependencies.jar -libjars <conf dir> -D xqueryparser.rules.file=xquery/parse-rules.xml <segment dir(s)> <result dir>

On a sigle URL:

	java  -cp "<conf dir>:target/nutch-xquery-parser-1.0-SNAPSHOT-jar-with-dependencies.jar" com.peer2gear.nutch.xquery.XQueryParser <url>
