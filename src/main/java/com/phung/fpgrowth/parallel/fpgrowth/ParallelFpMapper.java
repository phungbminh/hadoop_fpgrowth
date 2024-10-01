package com.phung.fpgrowth.parallel.fpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import com.phung.fpgrowth.FpGrowth;
import com.phung.fpgrowth.dto.Pair;
import com.phung.fpgrowth.utility.Helper;


public class ParallelFpMapper extends Mapper<Object,Text,IntWritable,Text>{
	private static List<Pair> fList = new ArrayList<Pair>();
	private final static IntWritable newKey = new IntWritable();
	private final static Text newValue = new Text();
	final static Logger logger = Logger.getLogger(ParallelFpMapper.class);

	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		logger.info("===================map=================");
		Map<String,Integer> gList = new HashMap<String,Integer>();
		Map<Integer,Boolean> visited = new HashMap<Integer,Boolean>();
		
		Configuration conf = context.getConfiguration();
		int noOfGroups = Integer.parseInt(conf.get("noOfGroups"));
		
		int totalElements = fList.size();
		logger.info("Flist.....");
		fList.forEach(item -> {
			logger.info("Flist key(item): " + item.getItem() + " value(count): " + item.getCount() );
		});
		if(noOfGroups >= totalElements) {
			for(int i = 0; i < totalElements; i++) {
				logger.info("Put gList: key " + fList.get(i).getItem() + " value: " + (i+1));
				gList.put(fList.get(i).getItem(), i+1);
			}
		}
		else {
			int elePerGroup = totalElements/noOfGroups;
			for(int i = 0; i<totalElements; i ++) {
				int groupId = (i + 1)/elePerGroup;
				if((i + 1)%elePerGroup != 0) {
					groupId++;
				}
				if(groupId > noOfGroups)
					groupId = noOfGroups;
				gList.put(fList.get(i).getItem(), groupId);
			}
		}
		List<String> transaction = Helper.reorderTransaction(new String(value.toString()), fList);
		logger.info("transaction " + String.join(" ", transaction));
		
		logger.info("Exporting mapper ...");
		for(int j = transaction.size() - 1;j >= 0; j--) {
			//logger.info("Duyet giao dich: " + String.join(" ", transaction));
			int gid = gList.get(transaction.get(j));
			//logger.info("gid: " + gid);
			if(!visited.containsKey(gid)) {
				//logger.info("gid: ==>" + "not visited");
				visited.put(gid, true);
				//logger.info("add " + "gid to visited" );
				//String logVisited = "";
				//for (Map.Entry<Integer,Boolean> entry : visited.entrySet()) {
				//	logVisited = logVisited + entry.getKey() +  " ";
				//}
				//logger.info("visited: " + logVisited);
				
				newKey.set(gid);
				StringBuilder dependentTransactions = new StringBuilder();
				for(int i = 0; i <= j; i++) {
					dependentTransactions.append(transaction.get(i));
					if(i != j)
						dependentTransactions.append(" ");
				}
				newValue.set(dependentTransactions.toString());
				logger.info("key: " + gid + " - value: " + dependentTransactions.toString());
				context.write(newKey, newValue);
			}
		}
	}
	public void setup(Context context) throws IOException,InterruptedException{
		fList = FpGrowth.readFList(context.getConfiguration());
	}
	
	
}
