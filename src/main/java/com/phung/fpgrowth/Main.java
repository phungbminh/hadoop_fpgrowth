package com.phung.fpgrowth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.phung.fpgrowth.utils.MemoryLogger;


public class Main {

	private static Map<String, Long> map = new HashMap<String, Long>();

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
		
		MemoryLogger.getInstance().reset();
		MemoryLogger.getInstance().checkMemory();
	
		String inputFile = args[0];
		String outputFile = args[1];
		int minimumSupport = Integer.parseInt(args[2]);

		map = FpGrowth.runFpGrowth(inputFile, outputFile, minimumSupport);
		printStats() ;

	}
	public static void printStats() {
		System.out.println("=============  FP-GROWTH - STATS ==================");
		System.out.println(" Max memory usage: " + MemoryLogger.getInstance().getMaxMemory() + " mb");
		System.out.println(" Frequent itemsets count : " + map.get("fListCount")); 
		System.out.println(" Time Counting: " + (map.get("parallelCounting") * 0.001));
		System.out.println(" Time Fp-Growth: " + (map.get("parallelFpGrowth") * 0.001));
		System.out.println(" Total time: " + ((map.get("parallelCounting") + map.get("parallelFpGrowth")) * 0.001) + " sec");
		System.out.println("===================================================");
	}

}
