package com.example.dep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepartureDelayCountJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		job.setJarByClass(DepartureDelayCountJob.class);
		
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
//		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");

		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");
		job.setInputFormatClass(TextInputFormat.class);		//생략가능
		
		job.setMapperClass(DepartureDelayCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(DepartureDelayCountReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setOutputFormatClass(TextOutputFormat.class);	//생략가능
		
		Path outputDir = new Path("/home/java/dataexpo_out/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
	}

}
