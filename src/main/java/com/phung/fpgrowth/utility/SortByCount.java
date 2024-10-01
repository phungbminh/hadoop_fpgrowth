package com.phung.fpgrowth.utility;

import java.util.Comparator;

import com.phung.fpgrowth.dto.Pair;


public class SortByCount implements Comparator<Pair>{
	
	public int compare(Pair a, Pair b) {
		if(a.getCount() == b.getCount()) {
			if(a.getItem().compareTo(b.getItem()) > 0)
				return 1;
			else
				return -1;
		}
		else if(a.getCount() > b.getCount())
			return -1;
		else
			return 1;
	}
}
