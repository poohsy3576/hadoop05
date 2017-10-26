package com.example.delay.mos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DelayCountJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		Job job = new Job(conf, "DelayCount");
		
		job.setJarByClass(DelayCountJob.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");

//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");
		job.setInputFormatClass(TextInputFormat.class);		
		
		job.setMapperClass(DelayCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(DelayCountReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setOutputFormatClass(TextOutputFormat.class);
//		Path outputDir = new Path("/home/java/dataexpo_out/1987");
		Path outputDir = new Path("/home/java/dataexpo_out/1988");
//		Path outputDir = new Path("/home/java/dataexpo_out/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//MultipleOutputs를 쓰려면 job을 제출하기 전에 설정해줘야 한다.
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrive", TextOutputFormat.class, Text.class, IntWritable.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		return job.waitForCompletion(true) ? 0 : -1;
		
	}

}
