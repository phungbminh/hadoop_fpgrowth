package com.phung.fpgrowth.parallel.counting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ParallelCountingReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	private final static LongWritable result = new LongWritable();
	final static Logger logger = Logger.getLogger(ParallelCountingReducer.class);
	
	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
		logger.info("================== reduce ==================");
		Configuration conf = context.getConfiguration();
		int minSupport = Integer.parseInt(conf.get("minSupport"));
		logger.info("key: " +  new String(key.getBytes()));
		
		long sum = 0;
		String valLog = "";
		for(LongWritable val : values) {
			valLog = valLog + val + " ";
			sum += val.get();
		}
		logger.info("value: " + valLog);
		
		if(sum >= minSupport) {
			result.set(sum);
			logger.info("key: " + key + " value: " + result);
			context.write(key, result);
		}
	}
}
