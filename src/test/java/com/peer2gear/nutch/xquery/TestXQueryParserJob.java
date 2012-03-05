/**
 * 
 */
package com.peer2gear.nutch.xquery;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.sax.SAXSource;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.parse.html.HtmlParser;
import org.apache.nutch.util.NutchConfiguration;
import org.ccil.cowan.tagsoup.Parser;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import com.peer2gear.nutch.xquery.XQueryParserJob;

/**
 * @author daniel
 *
 */
public class TestXQueryParserJob extends TestCase {
	
	public File createTempDirName(String suffix) throws IOException {
		File temp = File.createTempFile(getClass().getName(), suffix);
		if (!temp.delete()) throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
		return temp;
	}
	
	public void xtestHtmlParser() {
		Configuration conf = NutchConfiguration.create();
		HtmlParser htmlParser = new HtmlParser();
		htmlParser.setConf(conf);
	}
	
	public void xtestTagSoup() throws Exception {
		URL url = new URL("http://www.cannondale.com/2012/bikes/road/cyclocross/superx/2012-super-x-4-rival-21559");
	
		XMLReader reader = new Parser();
		reader.setFeature(Parser.namespacesFeature, false);
		reader.setFeature(Parser.namespacePrefixesFeature, false);
		reader.setFeature(Parser.ignoreBogonsFeature, true);
		reader.setFeature(Parser.bogonsEmptyFeature, false);
	
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
				
		DOMResult result = new DOMResult();
		transformer.transform(new SAXSource(reader, new InputSource(url.openStream())), result);
				
		// here we go - an DOM built from abitrary HTML
		result.getNode();
		System.out.println();
	}

	public void xtestParse() throws Exception {
		String inPath = getClass().getResource("/20120109123900/content").toString();
		Path[] inPaths = { new Path(inPath) };
		Path outPath = new Path(createTempDirName(".testParse").getAbsolutePath());
		XQueryParserJob job = new XQueryParserJob();
		Configuration conf = new Configuration();
		job.setConf(conf);
		job.parse(inPaths, outPath);
	}
	
	public void testNothing() {
	}

}
