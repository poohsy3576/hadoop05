package com.example.delay.mos;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.example.parser.AirlinePerformanceParser;
import com.example.parser.DelayCounters;

//workType 없이 departure, arrive delay 모두 구해서 reducer로 넘겨주자.
public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	final IntWritable one = new IntWritable(1);
	Text outputKey = new Text();

	// setup은 Mapper가 돌때 딱 한번만 호출된다.
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

		//출발지연
		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				//arrive와 구분해서 Reducer로 넘겨줘야 구분 가능
				outputKey.set("D, " + parser.getYear() + ", " + parser.getMonth());
				context.write(outputKey, one);
				context.getCounter(DelayCounters.delay_departure).increment(1);
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if (parser.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			} 
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
		
		//도착지연
		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				//departure와 구분해서 Reducer로 넘겨줘야 구분 가능
				outputKey.set("A, " +parser.getYear() + ", " + parser.getMonth());
				context.write(outputKey, one);
				context.getCounter(DelayCounters.delay_arrival).increment(1);
			}  else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
		
	}
}
