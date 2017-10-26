package com.example.delay;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//conf를 가지고 있는 Configured를 extends한다는 것은 DelayCountJob3가 conf를 field로 가지고 있다는 뜻!
public class DelayCountJob3 extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		//제너릭옵션파서를 만들고, 제너릭옵션파서가 conf를 만들고, 만들어진 configuration이 필드에 저장된다.
		ToolRunner.run(new DelayCountJob3(), args);
	}
	
	//제너릭옵션파서에서 제공하는 파라미터를 제외한 나머지를 처리한다.
	@Override
	public int run(String[] args) throws Exception {
		/* 
		 * 수행시 
		 * 		hadoop com.example.delay.DelayCountJob2 -DworkType=departure ==> 출발지연
		 * 		hadoop com.example.delay.DelayCountJob2 -DworkType=arrive ==> 도착지연
		 * 		
		 * 		==> configuration을 setting 할 필요가 없다.
		 */
		
		//제너릭옵션파서가 처리한 conf를 불러와서 사용
		String workType = getConf().get("workType");
		System.out.println("workType = " + workType);
	
		Configuration conf = getConf();
		
		Job job = new Job(conf, "DepartureDelayCount");
		
		job.setJarByClass(DelayCountJob3.class);
		
		FileInputFormat.setInputPaths(job, "/home/java/dataexpo/1987_nohead.csv");
		FileInputFormat.addInputPaths(job, "/home/java/dataexpo/1988_nohead.csv");

//		FileInputFormat.setInputPaths(job, "/home/java/dataexpo");
		job.setInputFormatClass(TextInputFormat.class);		//생략가능
		
		job.setMapperClass(DelayCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(DelayCountReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setOutputFormatClass(TextOutputFormat.class);	//생략가능
		Path outputDir = new Path("/home/java/dataexpo_out/1988");
//		Path outputDir = new Path("/home/java/dataexpo_out/total");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(outputDir, true);
		
		//job이 수행하고 결과가 true면 0, false면 -1을 리턴해라.
		return job.waitForCompletion(true) ? 0 : -1;
		
	}

	

}
