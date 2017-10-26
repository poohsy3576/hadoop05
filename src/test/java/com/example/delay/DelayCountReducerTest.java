package com.example.delay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DelayCountReducerTest {

	ReduceDriver<Text, IntWritable, Text, IntWritable> reduce;
	
	@Before
	public void setUp() throws Exception {
		reduce = ReduceDriver.newReduceDriver(new DelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testReduce() throws IOException {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		
		reduce.withInput(new Text("1987, 7"), values);
		reduce.withOutput(new Text("1987, 7"), new IntWritable(5));
		
		reduce.runTest();
	}

}
