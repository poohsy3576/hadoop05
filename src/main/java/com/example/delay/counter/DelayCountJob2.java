package com.example.delay.counter;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class DelayCountJob2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/* 
		 * 수행시 
		 * 		hadoop com.example.delay.DelayCountJob2 -DworkType=departure ==> 출발지연
		 * 		hadoop com.example.delay.DelayCountJob2 -DworkType=arrive ==> 도착지연
		 * 		
		 * 		==> configuration을 setting 할 필요가 없다.
		 */
		GenericOptionsParser parser = new GenericOptionsParser(args);
		
		if (args.length == 0) {
			//제너릭옵션을 수행하는 방법이 나온다.
			GenericOptionsParser.printGenericCommandUsage(System.out);
		}
		
		String workType = parser.getConfiguration().get("workType");
		System.out.println("workType = " + workType);

		Configuration conf = parser.getConfiguration();
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		job.setJarByClass(DelayCountJob2.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
//		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");

		//폴더를 지정하면 그 안에 있는게 다들어간다.
//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");
		job.setInputFormatClass(TextInputFormat.class);		//생략가능
		
		job.setMapperClass(DelayCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(DelayCountReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setOutputFormatClass(TextOutputFormat.class);	//생략가능
		Path outputDir = new Path("/home/java/dataexpo_out/1987");
//		Path outputDir = new Path("/home/java/dataexpo_out/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		job.waitForCompletion(true);
	}

}
