package com.example.arr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArriveDelayCountJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "ArriveDelayCount");
		
		job.setJarByClass(ArriveDelayCountJob.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");

		job.setMapperClass(ArriveDelayCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(ArriveDelayCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		Path outputDir = new Path("/home/java/dataexpo_arr/total");
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
		
	}

}
