package com.phung.fpgrowth.parallel.fpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;
import com.phung.fpgrowth.FpGrowth;
import com.phung.fpgrowth.dto.FpTree;
import com.phung.fpgrowth.dto.ListPair;
import com.phung.fpgrowth.dto.Pair;
import com.phung.fpgrowth.utility.Helper;

public class ParallelFpReducer extends Reducer<IntWritable, Text, Text, LongWritable> {
	private final static LongWritable value = new LongWritable();
	private static List<Pair> fList = new ArrayList<Pair>();
	private final static Text newKey = new Text();
	final static Logger logger = Logger.getLogger(ParallelFpReducer.class);

	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		logger.info("================== reduce ==================");
		Configuration conf = context.getConfiguration();
		int minSupport = Integer.parseInt(conf.get("minSupport"));
		int noOfGroups = Integer.parseInt(conf.get("noOfGroups"));
		int k = 1000;
		List<Pair> localFlist = new ArrayList<Pair>();
		ArrayList<String> transactions = new ArrayList<String>();
		List<String> localGList = new ArrayList<String>();
		Map<String,Integer> gList = new HashMap<String,Integer>();
		//Map<String,Boolean> localGList = new HashMap<String,Boolean>();
		
		for(Text val : values) {
			transactions.add(val.toString());
		}
		logger.info("key: " + key.get());
		logger.info("value: " + String.join(" ", transactions));
		
		int totalElements = fList.size();
		if(noOfGroups >= totalElements) {
			for(int i = 0; i < totalElements; i++) {
				gList.put(fList.get(i).getItem(), i+1);
				if(i+1 == key.get())
					localGList.add(fList.get(i).getItem());
					//localGList.put(fList.get(i).getItem(),true);
			}
		}
		else {
			int elePerGroup = totalElements/noOfGroups;
			for(int i=0;i<totalElements;i++) {
				int groupId = (i+1)/elePerGroup;
				if((i+1)%elePerGroup != 0) {
					groupId++;
				}
				if(groupId > noOfGroups)
					groupId = noOfGroups;
				gList.put(fList.get(i).getItem(), groupId);
				if(groupId == key.get())
					localGList.add(fList.get(i).getItem());
			}
		}
		logger.info("localGList: "+String.join(" ", localGList));
		logger.info("gList: ");
		gList.entrySet().stream().forEach(item -> {
			logger.info("key: " + item.getKey() + " value: " + item.getValue());
		});
		localFlist = Helper.makeFList(transactions, minSupport,gList);
		logger.info("localFlist");
		localFlist.stream().forEach(e -> {
			logger.info(e.getItem() + ":" + e.getCount());
		});
		FpTree tree = Helper.constructFpTree(transactions,localFlist);
	
		for (int i = 0; i < localGList.size(); i++) {
		    List<ListPair> patterns = new ArrayList<ListPair>();
		    patterns = Helper.mineFrequentPatterns(tree, minSupport, localGList.get(i), null, true);

		    for (int j = 0; j < patterns.size(); j++) {
		        Collections.sort(patterns.get(j).getItems());
		        logger.info(j + " - " + patterns.get(j).getItems().toString() + " : " + patterns.get(j).getCount());
		    }

		    /* add all patterns and their counts to a list */
		    List<ListPair> maxList = new ArrayList<ListPair>();
		    for (int j = 0; j < patterns.size(); j++) {
		        maxList.add(patterns.get(j));
		    }

		    // Sắp xếp danh sách theo thứ tự giảm dần của getCount()
		    Collections.sort(maxList, new Comparator<ListPair>() {
		        public int compare(ListPair p1, ListPair p2) {
		            return Long.compare(p2.getCount(), p1.getCount());
		        }
		    });

		    int curr = 0;
		    while (curr < k && curr < maxList.size()) {
		        ListPair p = maxList.get(curr);
		        newKey.set(p.getItems().toString() + " : ");
		        value.set(p.getCount());
		        context.write(newKey, value);
		        curr++;
		    }
		}
	}
	
	/* read F-list */
	public void setup(Context context) throws IOException,InterruptedException{
		fList = FpGrowth.readFList(context.getConfiguration());
	}

}
