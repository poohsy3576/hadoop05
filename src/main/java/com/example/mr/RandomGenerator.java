package com.example.mr;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RandomGenerator {

	public static void main(String[] args) throws IOException {

		/*
		 * << HADOOP1 에서 configuration이 읽는 파일들 >>
		 * conf/hadoop-env.sh
		 * conf/masters
		 * conf/slaves
		 * conf/core-site.xml
		 * conf/hdfs-site.xml
		 * conf/mapred-site.xml
		 */
		Configuration conf = new Configuration();
		
		FileSystem hdfs = FileSystem.get(conf);
		FSDataOutputStream out = hdfs.create(new Path("/home/java/mr/random.txt"));
		
		PrintWriter writer = new PrintWriter(out);

		Random r = new Random();
		
		for (int i=0; i<100; i++) {
			writer.printf("%010d\n", r.nextInt(100));
		}
		
		writer.close();
		hdfs.close();
		
		
	}

}
