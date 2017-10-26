package com.example.arr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArriveDelayCountMapperReducerTest {

	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapreduce;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,50,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String record3  = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record4 = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,50,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Before
	public void setUp() throws Exception {
		mapreduce = MapReduceDriver.newMapReduceDriver(new ArriveDelayCountMapper(), new ArriveDelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapReduce() throws IOException {
		mapreduce.withInput(new LongWritable(), new Text(record1));
		mapreduce.withInput(new LongWritable(), new Text(record2));
		mapreduce.withInput(new LongWritable(), new Text(record3));
		mapreduce.withInput(new LongWritable(), new Text(record4));
		
		mapreduce.withOutput(new Text("1987"), new IntWritable(1));
		mapreduce.withOutput(new Text("2008"), new IntWritable(2));
		
		mapreduce.runTest();
	}

}
