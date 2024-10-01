package com.phung.fpgrowth.parallel.counting;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.phung.fpgrowth.parallel.fpgrowth.ParallelFpMapper;

public class ParallelCountingMapper extends Mapper<Object,Text,Text,LongWritable>{
	
	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();
	final static Logger logger = Logger.getLogger(ParallelCountingMapper.class);
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		logger.info("================== map ==================");
		StringTokenizer str = new StringTokenizer(value.toString());
		while(str.hasMoreTokens()) {
			word.set(str.nextToken());
			logger.info("key: " + word + " value: " + one);
			context.write(word, one);
		}
	}
}
