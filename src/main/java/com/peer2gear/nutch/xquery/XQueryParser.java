/**
 * 
 */
package com.peer2gear.nutch.xquery;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQConstants;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQSequence;
import javax.xml.xquery.XQStaticContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author daniel
 *
 */
public class XQueryParser implements Configurable {
	public static final String XQUERYPARSER_RULES_FILE = "xqueryparser.rules.file";
	private static final String DEFAULT_XQUERY_DATA_SOURCE = "net.sf.saxon.xqj.SaxonXQDataSource";
	// private static final String DEFAULT_XQUERY_DATA_SOURCE = "ch.ethz.mxquery.xqj.MXQueryXQDataSource";
	/** My logger */
	private final static Log LOG = LogFactory.getLog(XQueryParser.class);
	private XQConnection xqConnection = null;
	private Configuration conf;
	private Map<String,List<XQueryIdentifier>> rules = new HashMap<String, List<XQueryIdentifier>>();

	private class XQueryIdentifier {
		public Pattern pattern;
		public XPathExpression xpath;
		public XQPreparedExpression expr;
		public XQueryIdentifier(Pattern pattern, XPathExpression xpath, XQPreparedExpression expr) {
			this.pattern = pattern;
			this.xpath = xpath;
			this.expr = expr;
		}		
	}

	public XQueryParser() {
		try {
			Class<?> xqDSClass = Class.forName(DEFAULT_XQUERY_DATA_SOURCE);
			XQDataSource ds = (XQDataSource) xqDSClass.newInstance();
			this.xqConnection = ds.getConnection();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) { LOG.error(e.getMessage()); }
			throw new RuntimeException(e.getMessage(), e);      
		}
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		try {
			this.parseRules();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) { LOG.error(e.getMessage()); }
			e.printStackTrace();
			throw new RuntimeException(e.getMessage(), e);      
		}
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	/**
	 * @param doc
	 * @param urlStr
	 * @param baseUrlStr
	 * @return
	 * @throws MalformedURLException
	 * @throws XQException
	 */
	public String parse(DocumentFragment doc, String urlStr, String baseUrlStr)
			throws MalformedURLException, XQException, XPathExpressionException {
		String parseOutput = null;
		XQPreparedExpression expr=null;// = this.matchURL(urlStr);
		LinkedList<XQueryIdentifier> potentialExpressions = this.matchURL(urlStr);
		if( potentialExpressions != null)
			expr = this.matchXPath(potentialExpressions, doc);

		if (expr != null) {
			expr.bindNode(XQConstants.CONTEXT_ITEM, doc, null);
			QName[] externalVariables = expr.getAllExternalVariables();
			for (QName qName: externalVariables) {
				if ("url".equals(qName.getLocalPart()))
					expr.bindString(new QName("url"), urlStr, null);
				if ("base_url".equals(qName.getLocalPart()))
					expr.bindString(new QName("base_url"), baseUrlStr, null);
			}
			XQSequence sequence = expr.executeQuery();
			parseOutput = sequence.getSequenceAsString(null);
		}
		return parseOutput;
	}

	public LinkedList<XQueryIdentifier> matchURL(String urlStr) throws MalformedURLException {
		URL url = new URL(urlStr);
		String domain = url.getHost();
		String pathAndQuery = url.getPath() + (url.getQuery() != null ? "?" + url.getQuery() : "");

		LinkedList<XQueryIdentifier> res = new LinkedList<XQueryIdentifier>();

		LinkedList<XQueryIdentifier> ruleList = (LinkedList<XQueryIdentifier>) this.rules.get(domain);
		if (null != ruleList) {
			for (XQueryIdentifier pair: ruleList) {
				Matcher matcher = pair.pattern.matcher(pathAndQuery);
				if (matcher.matches())
					res.add(pair);
			}
		}
		if(res.size()== 0){
			return null;
		}		
		return res;
	}

	public XQPreparedExpression matchXPath(LinkedList<XQueryIdentifier> potentialExpressions,DocumentFragment doc) throws XPathExpressionException{
		XQPreparedExpression ret = null;
		for(XQueryIdentifier pair : potentialExpressions){
			if(pair.xpath != null){
				String res =  (String)pair.xpath.evaluate(doc, XPathConstants.STRING);
				if(res != null && !res.equals("")){ // if result is non-empty, we have a match.
					ret = pair.expr;
					break;
				}
			}else{
				ret = pair.expr; // Use default (No xpath) if no other match is found.
			}
		}
		return ret;
	}

	public void printParseRules() {
		for (Entry<String, List<XQueryIdentifier>> domain : this.rules.entrySet()) {
			System.out.println(domain.getKey());
			for (XQueryIdentifier pair: domain.getValue()) {
				System.out.println("\t" + pair.pattern);
			}
		}
	}

	private void parseRules() throws Exception {
		String rulesFileName = this.getConf().get(XQUERYPARSER_RULES_FILE, "xquery/parse-rules.xml");
		URL rulesResource = this.getConf().getResource(rulesFileName);
		XQStaticContext ctx = this.xqConnection.getStaticContext();
		// Use the directory of the rule file as base path for XQuery
		ctx.setBaseURI(rulesResource.toString());
		this.xqConnection.setStaticContext(ctx);
		InputStream is = rulesResource.openStream();
		Document document = this.readXMLDocument(is);
		Element root = document.getDocumentElement();
		if (!"parse-rules".equals(root.getTagName())) {
			if (LOG.isErrorEnabled()) { LOG.error("No parse-rules element."); }
		}
		NodeList rules = root.getChildNodes();
		for (int i = 0; i < rules.getLength(); i++) {
			Node ruleNode = rules.item(i);
			if (!(ruleNode instanceof Element))
				continue;
			Element rule = (Element) ruleNode;
			if (!"rule".equals(rule.getTagName())) {
				if (LOG.isErrorEnabled()) { LOG.error("Non rule element."); }
			}
			String domain = rule.getAttribute("domain");
			String patternStr = rule.getAttribute("pattern");
			String xpathString = rule.getAttribute("xpath");
			XPathExpression xp=null;
			if(xpathString != null && !xpathString.equals("")){
				XPath xpath =  XPathFactory.newInstance().newXPath();
				// set namespace to http://www.w3.org/1999/xhtml
				xpath.setNamespaceContext(new SimpleNamespaceContext());
				xp = xpath.compile(xpathString);
			}

			String xquery = rule.getAttribute("xquery");
			if (!this.rules.containsKey(domain))
				this.rules.put(domain, new LinkedList<XQueryIdentifier>());
			URL resolvedXQueryPath = rulesResource.toURI().resolve(xquery).toURL();
			InputStream xqis = resolvedXQueryPath.openStream();
			XQPreparedExpression expr = this.xqConnection.prepareExpression(xqis);
			Pattern pattern = Pattern.compile(patternStr);
			this.rules.get(domain).add(new XQueryIdentifier(pattern, xp, expr));

			if (LOG.isDebugEnabled()) { LOG.debug("Parse rule: " + domain + " " + patternStr + " " + xquery); }
		}
	}

	private class SimpleNamespaceContext implements NamespaceContext{
		public String getNamespaceURI(String prefix){
			if(prefix != null && prefix.equals("h")){
				return "http://www.w3.org/1999/xhtml";
			}else{		    
				return null;
			}
		}

		public String getPrefix(String namespace){
			return null;
		}

		public Iterator getPrefixes(String namespace){
			return null;
		}
	}  


	private Document readXMLDocument(InputStream is) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document = builder.parse(is);
		return document;
	}

}
