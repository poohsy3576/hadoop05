package com.example.mr2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		
		LongWritable value = new LongWritable();
		
		for (LongWritable v : values) {
			sum += v.get();
		}
		
		//성능을 고려한다면 필드변수를 만들어서 new를 한 번만 한다.
		//context.write(key, new LongWritable(sum));
		
		value.set(sum);
		
		
		
		context.write(key, value);
	}
}
