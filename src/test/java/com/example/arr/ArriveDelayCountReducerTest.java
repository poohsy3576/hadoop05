package com.example.arr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArriveDelayCountReducerTest {
	
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduce;

	@Before
	public void setUp() throws Exception {
		reduce = ReduceDriver.newReduceDriver(new ArriveDelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> list = new ArrayList<>();
		list.add(new IntWritable(1));
		list.add(new IntWritable(1));
		
		reduce.withInput(new Text(), list);
		reduce.withOutput(new Text(), new IntWritable(2));
		
		reduce.runTest();

	}

}
