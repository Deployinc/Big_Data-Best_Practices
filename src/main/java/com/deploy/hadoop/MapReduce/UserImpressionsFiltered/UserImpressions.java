package com.deploy.hadoop.MapReduce.UserImpressionsFiltered;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserImpressions {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (args.length != 4) {
			usage();
		}
		
		if (!(args[2].equals("HOST") || args[2].equals("IDENTITY") || args[2].equals("USER")
				|| args[2].equals("TIME") || args[2].equals("REQUEST") || args[2].equals("STATUS")
				|| args[2].equals("SIZE") || args[2].equals("REFERER") || args[2].equals("AGENT"))) {
			usage();
		}
		
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Pass additional cmd line parameter to mapper and reducer
		conf.set("param", args[3]);
		
		ColumnsEnum column = null;
		if (args[2].equals("HOST")) {
			column = ColumnsEnum.HOST;
		} else if (args[2].equals("IDENTITY")) {
			column = ColumnsEnum.IDENTITY;
		} else if(args[2].equals("USER")) {
			column = ColumnsEnum.USER;
		} else if (args[2].equals("TIME")) {
			column = ColumnsEnum.TIME;
		} else if (args[2].equals("REQUEST")) {
			column = ColumnsEnum.REQUEST;
		} else if (args[2].equals("STATUS")) {
			column = ColumnsEnum.STATUS;
		} else if (args[2].equals("SIZE")) {
			column = ColumnsEnum.SIZE;
		} else if (args[2].equals("REFERER")) {
			column = ColumnsEnum.REFERER;
		} else if (args[2].equals("AGENT")) {
			column = ColumnsEnum.AGENT;
		}
		
		conf.setEnum("column", column);
		
		// Create job
		Job job = new Job(conf, "Visitors Count");
		job.setJarByClass(UserImpressionsMapper.class);

		// Setup MapReduce
		job.setMapperClass(UserImpressionsMapper.class);
		job.setReducerClass(UserImpressionsReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}
	
	private static void usage() {
		System.out.printf("Usage: hadoop jar PATH_TO_JAR_FILE com.deploy.hadoop.MapReduce.UserImpressionsFiltered.UserImpressions <input dir> <output dir> <column> <user-agent>\n");
		System.out.printf("Possible column parameters:\n");
		System.out.printf("HOST, IDENTITY, USER, TIME, REQUEST, STATUS, SIZE, REFERER, AGENT\n");
		System.exit(-1);
	}
}
