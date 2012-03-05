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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.w3c.dom.DocumentFragment;

/**
 * @author Daniel Fagerstrom
 *
 */
public class XQueryParseFilter implements HtmlParseFilter {
    public static final String METADATA_FIELD = "xquery-parser";

    /** My logger */
    private final static Log LOG = LogFactory.getLog(XQueryParseFilter.class);

    private XQueryParser xQueryParser;

    private Configuration conf;

    /* (non-Javadoc)
     * @see org.apache.nutch.parse.HtmlParseFilter#filter(org.apache.nutch.protocol.Content, org.apache.nutch.parse.ParseResult, org.apache.nutch.parse.HTMLMetaTags, org.w3c.dom.DocumentFragment)
     */
    @Override
    public ParseResult filter(Content content, ParseResult parseResult,
            HTMLMetaTags metaTags, DocumentFragment doc) {
        String urlStr = content.getUrl();
        String baseHref = metaTags.getBaseHref() != null ? metaTags.getBaseHref().toExternalForm(): urlStr;
        try {
            String parseOutput = xQueryParser.parse(doc, urlStr, baseHref);
            if (parseOutput != null && !"".equals(parseOutput)) {
                // get parse obj
                Parse parse = parseResult.get(urlStr);
                Metadata metadata = parse.getData().getParseMeta();
                metadata.add(METADATA_FIELD, parseOutput);
            }
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) { LOG.error(e.getMessage()); }
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);      
        }

        return parseResult;
    }

    public XQueryParseFilter() {
        xQueryParser = new XQueryParser();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.xQueryParser.setConf(conf);
    }

    public XQueryParser getXQueryParser() {
        return xQueryParser;
    }

    private static Content createContent(Configuration conf, String urlStr)
            throws FileNotFoundException, IOException {
        String contentType = "text/html";
        URL url = new URL(urlStr);
        URLConnection connection = url.openConnection();
        InputStream is = connection.getInputStream();
        byte bytes[] = IOUtils.toByteArray(is);
        Content content = new Content(urlStr, urlStr, bytes, contentType, new Metadata(), conf);
        return content;
    }

    private static void usage() {
        System.err.println("Usage: XQueryParseFilter <url> [segment]\n");           
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = NutchConfiguration.create();
        Content content = null;
        if (args.length < 1) {
            usage();
            return;
        }
        String urlStr = args[0];
        String segment = null;
        if (args.length == 2) {
            segment = args[1];
        }
        if (segment != null) {
            Path file = new Path(segment, Content.DIR_NAME);
            FileSystem fs = FileSystem.get(conf);
            System.out.println("path: " + file.toString());
            Reader[] readers = MapFileOutputFormat.getReaders(fs, file, conf);
            content = new Content();
            for (Reader reader: readers) {
                if (reader.get(new Text(urlStr), content) != null)
                    continue;
            }
            for (Reader reader: readers)
                reader.close();
        } else {
            content = createContent(conf, urlStr);                  
        }
        Parse parse =  new ParseUtil(conf).parse(content).get(content.getUrl());
        String result = parse.getData().getMeta(XQueryParseFilter.METADATA_FIELD);
        System.out.println(result);
    }
}
