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

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.HadoopFSUtil;

/**
 * @author daniel
 *
 */
public class ParseResult extends Configured implements Tool {
	
	public static class GetResultMapper extends Mapper<Text, ParseData, Text, Text> {
		public GetResultMapper() {}
		
		@Override
		protected void map(Text key, ParseData value, Context context)
				throws IOException, InterruptedException {
			String xqResult = value.getMeta(XQueryParseFilter.METADATA_FIELD);
			if (xqResult != null)
				context.write(key, new Text(xqResult));
		}
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] (<segment> ... | -dir <segments>) <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(getConf());
		for (int i = 0; i < args.length - 1; i++) {
			if ("-dir".equals(args[i])) {
		        Path dir = new Path(args[++i]);
		        FileSystem fs = dir.getFileSystem(getConf());
		        FileStatus[] fstats = fs.listStatus(dir,
		                HadoopFSUtil.getPassDirectoriesFilter(fs));
		        Path[] segments = HadoopFSUtil.getPaths(fstats);
		        for (Path segment: segments) {
		    		FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
		        }
			} else {
				FileInputFormat.addInputPath(job, new Path(args[i], ParseData.DIR_NAME));				
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(GetResultMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0: 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ParseResult(), args);
		System.exit(exitCode);
	}

}
