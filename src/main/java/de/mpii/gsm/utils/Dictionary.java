package de.mpii.gsm.utils;


import de.mpii.gsm.utils.PrimitiveUtils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

public class Dictionary {
	
	//Combines itemId and support value in a long value
	protected ArrayList<Long> items = new ArrayList<Long>();
	
	protected int[] parentIds;
	
	OpenIntObjectHashMap<String> itemIdToItemMap = new OpenIntObjectHashMap<String>();
	
	public void load(Configuration conf, String fileName, int minSupport) throws IOException {
		
		BufferedReader br = null;
		if (conf == null) {
			@SuppressWarnings("resource")
			FileInputStream fstream = new FileInputStream(fileName);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
		} else {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream dis = fs.open(new Path(fileName));
			br = new BufferedReader(new InputStreamReader(dis));
		}
		
		OpenIntIntHashMap parentMap = new OpenIntIntHashMap();
		
		String line = null;

		while ((line = br.readLine()) != null) {
			String[] splits = line.split("\t");
			int itemId = Integer.parseInt(splits[3]);
			int itemSupport = Integer.parseInt(splits[2]);
			
			int parentId = Integer.parseInt(splits[4]);
			
			parentMap.put(itemId, parentId);

			if (itemSupport >= minSupport) {
				items.add(PrimitiveUtils.combine(itemId, itemSupport));
			}
			itemIdToItemMap.put(itemId, splits[0]);
		}
		
		Collections.sort(items, new MyComparator());
		
		parentIds = new int[parentMap.size() + 1];
		IntArrayList keyList = parentMap.keys();
		for(int i = 0; i < keyList.size(); ++i) {
			int item = keyList.get(i); 
			parentIds[item] = parentMap.get(item);
		}
	}
	
	public OpenIntObjectHashMap<String> getItemIdToName(){
		return itemIdToItemMap;
	}
	
	//Items are sorted in decreasing order of frequencies
	public ArrayList<Long> getItems(){
		return items;
	}
	
	
	public int[] getItemToParent(){
		return parentIds;
	}
	
	public static void main(String[] args) throws IOException {
	}

}
