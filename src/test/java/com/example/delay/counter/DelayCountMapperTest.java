package com.example.delay.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.example.parser.DelayCounters;

public class DelayCountMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> map;
	Configuration conf;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,9,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,40,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";

	@Before
	public void setUp() throws Exception {
		map = MapDriver.newMapDriver(new DelayCountMapper());
		conf = new Configuration();
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMap1() throws IOException {
		conf.setStrings("workType", "arrive");
		
		map.setConfiguration(conf);
		map.withInput(new LongWritable(0), new Text(record1));
//		map.withOutput(new Text("2008, 1"), new IntWritable(1));
		map.withCounter(DelayCounters.delay_arrival, 0);
		map.withCounter(DelayCounters.early_arrival, 0);
		map.withCounter(DelayCounters.scheduled_arrival, 0);
		map.withCounter(DelayCounters.not_available_arrival, 1);
		
		map.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMap2() throws IOException {
		conf.setStrings("workType", "arrive");
		
		map.setConfiguration(conf);
		map.withInput(new LongWritable(0), new Text(record2));
		map.withOutput(new Text("2008, 9"), new IntWritable(1));
		map.withCounter(DelayCounters.delay_arrival, 1);
		map.withCounter(DelayCounters.early_arrival, 0);
		map.withCounter(DelayCounters.scheduled_arrival, 0);
		map.withCounter(DelayCounters.not_available_arrival, 0);
		
		map.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMap3() throws IOException {
		conf.setStrings("workType", "departure");
		
		map.setConfiguration(conf);
		map.withInput(new LongWritable(0), new Text(record1));
		map.withOutput(new Text("2008, 1"), new IntWritable(1));
		map.withCounter(DelayCounters.delay_departure, 1);
		map.withCounter(DelayCounters.early_departure, 0);
		map.withCounter(DelayCounters.scheduled_departure, 0);
		map.withCounter(DelayCounters.not_available_departure, 0);
		
		map.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMap4() throws IOException {
		conf.setStrings("workType", "departure");
		
		map.setConfiguration(conf);
		map.withInput(new LongWritable(0), new Text(record2));
//		map.withOutput(new Text("2008, 9"), new IntWritable(1));
		map.withCounter(DelayCounters.delay_departure, 0);
		map.withCounter(DelayCounters.early_departure, 0);
		map.withCounter(DelayCounters.scheduled_departure, 0);
		map.withCounter(DelayCounters.not_available_departure, 1);
		
		map.runTest();
	}

}
