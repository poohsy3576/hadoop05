package com.example.delay.mos;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.example.parser.DelayCounters;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DelayCountMapper.class, DelayCountReducer.class})
public class DelayCountMapperReducerTest {

	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduce;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,60,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String record3  = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record4 = "1987,7,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,50,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	
	@Before
	public void setUp() throws Exception {
		mapReduce = MapReduceDriver.newMapReduceDriver(new DelayCountMapper(), new DelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapReduce1() throws IOException {
		mapReduce.withInput(new LongWritable(), new Text(record1));
		mapReduce.withInput(new LongWritable(), new Text(record2));
		mapReduce.withInput(new LongWritable(), new Text(record3));
		mapReduce.withInput(new LongWritable(), new Text(record4));
		
		mapReduce.withOutput(new Text("A, 1987, 1"), new IntWritable(1));
		mapReduce.withOutput(new Text("A, 2008, 1"), new IntWritable(2));
		mapReduce.withOutput(new Text("D, 1987, 1"), new IntWritable(1));
		mapReduce.withOutput(new Text("D, 1987, 7"), new IntWritable(1));
		mapReduce.withOutput(new Text("D, 2008, 1"), new IntWritable(1));
		
		mapReduce.withCounter(DelayCounters.delay_arrival, 3);
		mapReduce.withCounter(DelayCounters.delay_departure, 3);
		
		mapReduce.runTest();
	}
	
}
