package com.example.delay.mos;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable outValue = new IntWritable();

	MultipleOutputs<Text, IntWritable> mos;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		outValue.set(sum);

		context.write(key, outValue);	// part-r-00000 (D, A 둘다 나옴)
		
		String[] keys = key.toString().split(", ");
		if (keys[0].equals("D")) {
			mos.write("departure", new Text(keys[1] + ", " + keys[2]), outValue);	// departure-r-00000 (only D)
		}
		if (keys[0].equals("A")) {
			mos.write("arrive", new Text(keys[1] + ", " + keys[2]), outValue);	// arrive-r-00000 (only A)
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
