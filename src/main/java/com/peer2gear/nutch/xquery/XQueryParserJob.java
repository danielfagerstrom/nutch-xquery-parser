/**
 * 
 */
package com.peer2gear.nutch.xquery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;

/**
 * @author daniel
 *
 */
public class XQueryParserJob extends Configured implements Tool {
	private final static Log LOG = LogFactory.getLog(XQueryParserJob.class);

	public static class XQueryMapper extends Mapper<Text, Content, Text, Text> {
		XQueryParser xQueryParser;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			xQueryParser = new XQueryParser();
			xQueryParser.setConf(context.getConfiguration());
		}

		@Override
		public void map(Text key, Content content, Context context)
				throws IOException, InterruptedException {
			String resultStr = null;
			InputStream input = new ByteArrayInputStream(content.getContent());
            String urlString = key.toString();
			try {
                resultStr = xQueryParser.parseStream(input, urlString);
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) { LOG.error(e.getMessage()); }
				e.printStackTrace();
				throw new RuntimeException(e.getMessage(), e);      
			} finally {
				input.close();
			}
			context.write(key, new Text(resultStr));
		}

	}

	/**
	 * @param inPaths
	 * @param outPath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public int parse(Path[] inPaths, Path outPath) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job = new Job(getConf());
		for (Path inPath: inPaths) {
			FileInputFormat.addInputPath(job, inPath);			
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setJarByClass(XQueryParserJob.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(XQueryMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0: 1;
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

		ArrayList<Path> inPaths = new ArrayList<Path>();
		for (int i = 0; i < args.length - 1; i++) {
			if ("-dir".equals(args[i])) {
		        Path dir = new Path(args[++i]);
		        FileSystem fs = dir.getFileSystem(getConf());
		        FileStatus[] fstats = fs.listStatus(dir,
		                HadoopFSUtil.getPassDirectoriesFilter(fs));
		        Path[] segments = HadoopFSUtil.getPaths(fstats);
		        for (Path segment: segments) {
		        	inPaths.add(new Path(segment, Content.DIR_NAME));
		        }
			} else {
	        	inPaths.add(new Path(args[i], Content.DIR_NAME));
			}
		}
		Path outPath = new Path(args[args.length - 1]);

		return parse(inPaths.toArray(new Path[inPaths.size()]), outPath);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new XQueryParserJob(), args);
		System.exit(exitCode);
	}

}
