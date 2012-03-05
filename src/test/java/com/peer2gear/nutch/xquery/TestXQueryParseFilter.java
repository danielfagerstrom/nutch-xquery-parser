/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.peer2gear.nutch.xquery;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;

import com.peer2gear.nutch.xquery.XQueryParseFilter;
import com.peer2gear.nutch.xquery.XQueryParser;

/**
 * @author daniel
 *
 */
public class TestXQueryParseFilter extends TestCase {

	// This system property is defined in ./src/plugin/build-plugin.xml
	private String sampleDir = System.getProperty("test.data", ".");
	private XQueryParseFilter xQueryParseFilter;
	private Configuration conf;

	public TestXQueryParseFilter(String name) {
		super(name);
	}
	
	private Document readXMLDocument(InputStream is) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document = builder.parse(is);
		return document;
	}
	
	private DocumentFragment readXMLFragment(InputStream is) throws Exception {
		Document document = this.readXMLDocument(is);
		DocumentFragment fragment = document.createDocumentFragment();
		fragment.appendChild(document.getDocumentElement());
		return fragment;
	}
	
	private void addConfigRules() {
		conf.set(XQueryParser.XQUERYPARSER_RULES_FILE, "data/parse-rules.xml");
	}

	public void createXQueryParser() throws Exception {
		conf = new Configuration();
		addConfigRules();
		this.xQueryParseFilter = new XQueryParseFilter();
		this.xQueryParseFilter.setConf(conf);
		this.xQueryParseFilter.getXQueryParser().printParseRules();
	}

	private Content createContent(Configuration conf)
			throws FileNotFoundException, IOException {
		String url = "http://test.org/";
	    String contentType = "text/html";
	    File file = new File(sampleDir, "kenmore-microwave-17in.html");
	    FileInputStream is = new FileInputStream(file);
	    byte bytes[] = new byte[(int) file.length()];
	    is.read(bytes);
	    Content content = new Content(url, url, bytes, contentType, new Metadata(), conf);
		return content;
	}

	public void xtestParse() throws Exception {
		this.createXQueryParser();
	    Content content = this.createContent(conf);
		DocumentFragment doc = this.readXMLFragment(new ByteArrayInputStream(content.getContent()));
		String parseResult = this.xQueryParseFilter.getXQueryParser().parse(doc, content.getUrl(), content.getUrl());
		System.out.println(parseResult);
	}
	
	public void xtestFilter() throws Exception {
		this.createXQueryParser();
	    Content content = this.createContent(conf);
		DocumentFragment doc = this.readXMLFragment(new ByteArrayInputStream(content.getContent()));
		ParseResult parseResult = ParseResult.createParseResult(content.getUrl(), new ParseImpl("", new ParseData()));
		HTMLMetaTags metaTags = null;
		this.xQueryParseFilter.filter(content, parseResult, metaTags, doc);
	    Parse parse = parseResult.get(content.getUrl());
	    Metadata metadata = parse.getData().getParseMeta();
	    String parseOutput = metadata.get(XQueryParseFilter.METADATA_FIELD);
		System.out.println(parseOutput);
	}
	
	public void xtestIt() throws Exception {
	    conf = NutchConfiguration.create();
	    addConfigRules();
		Content content = createContent(conf);
	    Parse parse =  new ParseUtil(conf).parse(content).get(content.getUrl());
	    String result = parse.getData().getMeta(XQueryParseFilter.METADATA_FIELD);
	    System.out.println(result);
	}

	public void testNothing() {
	}
}
