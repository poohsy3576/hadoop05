package com.example.dep;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirlinePerformanceParser;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	final IntWritable one = new IntWritable(1);
	Text outputKey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		// true일때만 이용가능하도록
		if (parser.isDepartureDelayAvailable()) {
			//출발 지연 시간이 0보다 큰 지연의 case만 다루겠다.
			if (parser.getDepartureDelayTime() > 0) {
				outputKey.set(parser.getYear() + "");
				context.write(outputKey, one);
			}
		}
	}
}
