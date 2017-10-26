package com.example.delay.counter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.example.parser.DelayCounters;

public class DelayCountMapperReducerTest {

	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduce;
	Configuration conf;
	
	String record1  = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,34,34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record2 = "2008,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,60,NA,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	String record3  = "1987,1,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,-34,-34,IND,BWI,515,3,10,0,,0,2,0,0,0,32";
	String record4 = "1987,7,3,4,1829,1755,1959,1925,WN,3920,N464WN,90,90,77,NA,0,IND,BWI,NA,3,10,0,,0,2,0,0,0,32";
	
	@Before
	public void setUp() throws Exception {
		mapReduce = MapReduceDriver.newMapReduceDriver(new DelayCountMapper(), new DelayCountReducer());
		conf = new Configuration();
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMapReduce1() throws IOException {
		conf.setStrings("workType", "arrive");
		
		mapReduce.setConfiguration(conf);
		mapReduce.withInput(new LongWritable(), new Text(record1));
		mapReduce.withInput(new LongWritable(), new Text(record2));
		mapReduce.withInput(new LongWritable(), new Text(record3));
		mapReduce.withInput(new LongWritable(), new Text(record4));
		
//		mapReduce.withOutput(new Text("1987, 1"), new IntWritable(1));
//		mapReduce.withOutput(new Text("1987, 7"), new IntWritable(1));
		mapReduce.withOutput(new Text("2008, 1"), new IntWritable(2));
		
		mapReduce.withCounter(DelayCounters.delay_arrival, 2);
		mapReduce.withCounter(DelayCounters.early_arrival, 1);
		mapReduce.withCounter(DelayCounters.scheduled_arrival, 0);
		mapReduce.withCounter(DelayCounters.not_available_arrival, 1);
		
		mapReduce.runTest();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testMapReduce2() throws IOException {
		conf.setStrings("workType", "departure");
		
		mapReduce.setConfiguration(conf);
		mapReduce.withInput(new LongWritable(), new Text(record1));
		mapReduce.withInput(new LongWritable(), new Text(record2));
		mapReduce.withInput(new LongWritable(), new Text(record3));
		mapReduce.withInput(new LongWritable(), new Text(record4));
		
//		mapReduce.withOutput(new Text("1987, 1"), new IntWritable(1));
//		mapReduce.withOutput(new Text("1987, 7"), new IntWritable(1));
		mapReduce.withOutput(new Text("2008, 1"), new IntWritable(1));
		
		mapReduce.withCounter(DelayCounters.delay_departure, 1);
		mapReduce.withCounter(DelayCounters.early_departure, 1);
		mapReduce.withCounter(DelayCounters.scheduled_departure, 1);
		mapReduce.withCounter(DelayCounters.not_available_departure, 1);
		
		mapReduce.runTest();
	}

}
