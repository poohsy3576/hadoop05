package com.example.arr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirlinePerformanceParser;

public class ArriveDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	final IntWritable one = new IntWritable(1);
	Text outputkey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				outputkey.set(parser.getYear() + "");
				context.write(outputkey, one);
			}
		}
	}
}
