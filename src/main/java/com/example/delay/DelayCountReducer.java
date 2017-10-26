package com.example.delay;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable outValue = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)	throws IOException, InterruptedException {

		//Reducer는 도착지연인지 출발지연인지 구분할 필요가 없다. Mapper에서 넘어온 값들을 합하기만 하면 될 뿐!
		int sum=0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		
		outValue.set(sum);
		context.write(key, outValue);
	}
}
