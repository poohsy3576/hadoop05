package com.example.mr2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SortJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(new Configuration(), "Sort Test 2");

		job.setJarByClass(SortJob.class);
		
		FileInputFormat.setInputPaths(job, new Path("/home/java/mr/random.txt"));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapperClass(SortMapper.class);
		
		//mapoutput 설정은 아래의 리듀서 설정과 같으면 생략가능
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setSortComparatorClass(TextGroupingComparator.class);

		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/home/java/mr/out"));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		hdfs.delete(new Path("/home/java/mr/out"), true);
		
		job.waitForCompletion(true);
	}

}
