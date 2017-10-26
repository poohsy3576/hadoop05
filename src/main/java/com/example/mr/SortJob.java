package com.example.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

		Job job = new Job(new Configuration(), "Sort Test");

		/*
		 * 1. Jar  Class 결정
		 */
		job.setJarByClass(SortJob.class);
		
		/*
		 * 2. InputFormat 결정
		 */
		//경로 지정
		FileInputFormat.setInputPaths(job, new Path("/home/java/mr/random.txt"));
		//random.txt 파일에서 keyvalue 형태로 읽어서 mapper에게 전해줘. (위 파일은 한줄이므로 key에 값이 들어가고 value=null로 저장되서 넘어간다.)
		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setInputFormatClass(TextInputFormat.class);
		
		/*
		 * 3. Mapper Class
		 */
		//우리가 안만들었으니까 기본 mapper를 쓴다. (입력으로 들어온 것을 그대로 출력하는 mapper) ==> Identity Mapper
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//default sorting ==> ascending 
		job.setSortComparatorClass(TextGroupingComparator.class);
		
		/*
		 * 4. Reduce Class
		 */
		//Identity Reducer ==> 기본적으로 sort된다.
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		/*
		 * 5. OutputFormat 결정
		 */
		FileOutputFormat.setOutputPath(job, new Path("/home/java/mr/out"));
		//key value로 넘어온 것을 text로 써라.
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		hdfs.delete(new Path("/home/java/mr/out"), true);
		
		job.waitForCompletion(true);
	}

}
