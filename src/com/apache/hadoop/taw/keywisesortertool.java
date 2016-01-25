package com.apache.hadoop.taw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class keywisesortertool extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out
				.println("************************** In main method ****************");

		Configuration configuration = new Configuration();
		keywisesortertool dateTR = new keywisesortertool();
	
		int exitCode = ToolRunner.run(configuration, dateTR, args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		if (arg0.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf,"Word Count1");
		//job.setNumReduceTasks(0);
		System.out
				.println("************************** After property populator ****************");
		job.setJarByClass(keywisesortertool.class);
		job.setMapperClass(keywisesortermapper.class);
		job.setReducerClass(keywisesorterreducer.class);
		//job.setCombinerClass(reducertask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}

