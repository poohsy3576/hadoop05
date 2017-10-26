package com.example.dep;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DepartureDelayCountMapperReducerTest {

	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduce;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String record3  = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record4 = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,50,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	
	@Before
	public void setUp() throws Exception {
		mapReduce = MapReduceDriver.newMapReduceDriver(new DepartureDelayCountMapper(), new DepartureDelayCountReducer());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduce.withInput(new LongWritable(), new Text(record1));
		mapReduce.withInput(new LongWritable(), new Text(record2));
		mapReduce.withInput(new LongWritable(), new Text(record3));
//		mapReduce.withInput(new LongWritable(), new Text(record4));
		
		mapReduce.withOutput(new Text("1987"), new IntWritable(1));
		mapReduce.withOutput(new Text("2008"), new IntWritable(1));
		
		mapReduce.runTest();
	}

}
