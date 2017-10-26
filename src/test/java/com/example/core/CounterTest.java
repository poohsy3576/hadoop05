package com.example.core;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.Test;

public class CounterTest {

	static enum XXX {
		cnt, cnt2;
	}
	
	static class MyCounterMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
		//cluster 전체를 counting
		@Override
		protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			//value%10 만큼만 카운팅하겠다.
			for (int i=0; i<value.get()%10; i++) {
				//XXX 그룹에 cnt가 없으면 만들어지고, 그 값을 1만 증가시켜라.
				context.getCounter("XXX", "cnt").increment(1);
				
				//enum으로 만들어 놓고 사용하면 문법체크가 되서 좋다.
				context.getCounter(XXX.cnt2).increment(2);
			}
			
			context.write(key, value);
			
		}

	}

	MapDriver<Text, IntWritable, Text, IntWritable> mapDriver;
	@Before
	public void setUp() throws Exception {
		mapDriver = MapDriver.newMapDriver(new MyCounterMapper());
	}

	@Test
	public void testMap() throws IOException {
		//Method Chain 방식으로 설계되있기 때문에 이렇게 한 줄로 써도 된다.
		mapDriver.withInput(new Text("java"), new IntWritable(14))
		         .withOutput(new Text("java"), new IntWritable(14))
		         .withCounter("XXX", "cnt", 4)
		         .withCounter(XXX.cnt2, 8)
		         .runTest();
		
	}
	
	@Test
	public void testMap2() throws IOException {
		//Method Chain 방식으로 설계되있기 때문에 이렇게 한 줄로 써도 된다.
		mapDriver.withInput(new Text("java"), new IntWritable(14))
		         .withOutput(new Text("java"), new IntWritable(14))
		         .withCounter("XXX", "cnt", 4)
//		         .withCounter(XXX.cnt2, 8)
		         .withCounter("com.example.core.CounterTest$XXX","cnt2", 8)
		         .runTest();
		
	}
	
	@Test
	public void testMethodChaining() throws IOException {
		String result = "Hello World".toLowerCase()
									     .substring(0, 5)
									     .concat(" Java !")
									     .toUpperCase();
		
//		assertThat(result, is("hello world"));
//		assertThat(result, is("hello"));
//		assertThat(result, is("hello Java !"));
		assertThat(result, is("HELLO JAVA !"));
	}
	

}
