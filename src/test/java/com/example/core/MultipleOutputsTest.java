package com.example.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

//multipleoutputs를 테스트 하기 위해서는 annotation이 필요
@RunWith(PowerMockRunner.class)
@PrepareForTest(MultipleOutputsTest.MyReducer.class)
public class MultipleOutputsTest {

	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
				context.getCounter("MyCount", "cnt").increment(1);
			}
			
			mos.write("departure", key, new IntWritable(sum * 10));	// departure-r-00000
			mos.write("arrive", key, new IntWritable(sum * 100));	// arrive-r-00000
			
			context.write(key, new IntWritable(sum));	// part-r-00000
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() throws Exception {
		reduceDriver = ReduceDriver.newReduceDriver(new MyReducer());
	}

	@Test
	public void testReduce() throws IOException {
		Text key = new Text("xxx");
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		reduceDriver.withInput(key, values)
					.withOutput(key, new IntWritable(3))
					.withCounter("MyCount", "cnt", 3)
					.withMultiOutput("departure", key, new IntWritable(30))
					.withMultiOutput("arrive", key, new IntWritable(300))
					.runTest();

	}

}
