package com.example.arr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ArriveDelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable outputValue = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum=0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		outputValue.set(sum);
		context.write(key, outputValue);
	}
}
