package com.example.arr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArriveDelayCountMapperTest {

	MapDriver<LongWritable, Text, Text, IntWritable> map;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Before
	public void setUp() throws Exception {
		map = MapDriver.newMapDriver(new ArriveDelayCountMapper());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapperAvailable() throws IOException {
		map.withInput(new LongWritable(0), new Text(record1));
		map.withOutput(new Text("2008"), new IntWritable(1));
		map.runTest();
	}
	
	@Test
	public void testMapperNA() throws IOException {
		map.withInput(new LongWritable(), new Text(record2));
//		map.withOutput(new Text("2008"), new IntWritable(0));
		map.runTest();
	}

}
