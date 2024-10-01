package com.phung.fpgrowth;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.phung.fpgrowth.dto.Pair;
import com.phung.fpgrowth.parallel.counting.ParallelCountingMapper;
import com.phung.fpgrowth.parallel.counting.ParallelCountingReducer;
import com.phung.fpgrowth.parallel.fpgrowth.ParallelFpMapper;
import com.phung.fpgrowth.parallel.fpgrowth.ParallelFpReducer;
import com.phung.fpgrowth.utility.SortByCount;





public class FpGrowth {
	
	private final static int noOfGroups = 1000;
	final static Logger logger = Logger.getLogger(FpGrowth.class);
	private static long startTimesCounting;
	private static long endTimeCounting;
	private static long startTimesFpGrowth;
	private static long endTimeFpGrowth;
	
	protected static Map<String, Long> runFpGrowth(String inputFile,String outputFile,int minSupport) throws IOException, ClassNotFoundException , InterruptedException{
		Map<String, Long> map = new HashMap<String, Long>();
		Configuration conf = new Configuration();
		conf.set("outputFile", outputFile);
		conf.set("noOfGroups", String.valueOf(noOfGroups));
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "51200"); // 50KB
		conf.set("mapreduce.input.fileinputformat.split.minsize", "25600"); // 25KB
		startTimesCounting = System.currentTimeMillis();
		startParallelCounting(conf,inputFile,outputFile,minSupport);
		List<Pair> fList = getFList(outputFile);
		saveFlist(fList,conf,outputFile);
		
		endTimeCounting = System.currentTimeMillis();
		startTimesFpGrowth = System.currentTimeMillis();
		startParallelFpGrowth(conf,inputFile,outputFile);
		endTimeFpGrowth = System.currentTimeMillis();
		map.put("parallelCounting", endTimeCounting - startTimesCounting);
		map.put("parallelFpGrowth", endTimeFpGrowth - startTimesFpGrowth);
		map.put("fListCount", Long.parseLong(String.valueOf(fList.size())));
		
		return map;
	}
	/* reads items and count from HDFS after 1st map reduce, sorts it according to count(desc) and returns it*/
	public static List<Pair> getFList(String outputFile) {
		try {
			Path p = new Path(outputFile + "/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line;
            line = br.readLine();
            List<Pair> fList = new ArrayList<Pair>();
            while (line != null){
            	StringTokenizer s1 = new StringTokenizer(line);
            	String item = s1.nextToken();
            	long count  = Long.parseLong(s1.nextToken());
            	fList.add(new Pair(item,count));
            	line = br.readLine();
            }
            Collections.sort(fList,new SortByCount());
            return fList;
		}
		catch(Exception e) {
			System.out.println(e);
			return null;
		}
	}
	
	/* save F-list to hdfs in a file */
	protected static void saveFlist(List<Pair> fList,Configuration conf,String outputFile) throws IOException {
		String destination = outputFile + "/fList";
		FileSystem fs = FileSystem.get(URI.create(destination), conf);
		OutputStream out = fs.create(new Path(destination));
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter(out) );
		for(Pair itemAndCount: fList) {
			br.write(itemAndCount.getItem() + " " + itemAndCount.getCount() + "\n");
		}
		br.close();
		fs.close();
	}
	
	/* read F-list from hdfs */
	public static List<Pair> readFList(Configuration conf) throws IOException{
		Path p = new Path(conf.get("outputFile") + "/fList");
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line = br.readLine();
        List<Pair> fList = new ArrayList<Pair>();
        while (line != null){
        	StringTokenizer s1 = new StringTokenizer(line);
        	String item = s1.nextToken();
        	long count  = Long.parseLong(s1.nextToken());
        	fList.add(new Pair(item, count));
        	logger.info(item + " " + count);
        	line = br.readLine();
        }
        return fList;
		
	}
	protected static void startParallelFpGrowth(Configuration conf,String inputFile,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		Job job = new Job(conf,"Phase2: FpGrowth");
		job.setJarByClass(FpGrowth.class);
		job.setMapperClass(ParallelFpMapper.class);
	   	job.setReducerClass(ParallelFpReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	job.setMapOutputKeyClass(IntWritable.class);
	   	job.setMapOutputValueClass(Text.class);
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile + "_result"));
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	
	/* get count of all items in transactions */
	protected static void startParallelCounting(Configuration conf,String inputFile,String outputFile,int minSupport) throws IOException,InterruptedException,ClassNotFoundException{
		conf.set("minSupport", String.valueOf(minSupport));
		Job job = new Job(conf, "Phase1: Counting");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(ParallelCountingMapper.class);
	   	job.setReducerClass(ParallelCountingReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	
	
}
