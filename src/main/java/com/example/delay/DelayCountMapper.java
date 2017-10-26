package com.example.delay;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirlinePerformanceParser;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final IntWritable one = new IntWritable(1);
	Text outputKey = new Text();

	String workType;

	// setup은 Mapper가 돌때 딱 한번만 호출된다.
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//설정에서 workType을 가져온다.
		workType = context.getConfiguration().get("workType");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

//		equals를 거꾸로 쓰면 NullPointerException이 나지 않는다.
		if ("departure".equals(workType)) {
			//출발지연
			if (parser.isDepartureDelayAvailable()) {
				if (parser.getDepartureDelayTime() > 0) {
					outputKey.set(parser.getYear() + ", " + parser.getMonth());
					context.write(outputKey, one);
				}
			}
		} else {
			//도착지연
			if (parser.isArriveDelayAvailable()) {
				if (parser.getArriveDelayTime() > 0) {
					outputKey.set(parser.getYear() + ", " + parser.getMonth());
					context.write(outputKey, one);
				}
			}
		}
	}
}
