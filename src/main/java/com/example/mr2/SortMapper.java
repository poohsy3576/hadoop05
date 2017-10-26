package com.example.mr2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, Text, LongWritable> {
	
	static final LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		//file line수가 적으면 계속 new 해도 되지만 성능을 고려하면 좋지 않은 방법이다.
		//context.write(key, new LongWritable(1));
		
		context.write(key, one);
		
	}
}
