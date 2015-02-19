package de.mpii.gsm.taxonomy;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class NytTaxonomy implements Taxonomy{
	
	int[] parentIds = null;
	
	int maxDepth = 0;
	
	public NytTaxonomy(){};
	
	public NytTaxonomy(int[] p) {
		this.parentIds = p;
	}
	
	public NytTaxonomy(String fileName) throws Exception{
		load(fileName);
	}
	
	
	public void load(int[] p){
		this.parentIds = p;
	}
	
	@Override
	public void load(String fileName) throws Exception {

		FileInputStream fis = new FileInputStream(fileName);
		ObjectInputStream ois = new ObjectInputStream(fis);
		int length = ois.readInt();
		parentIds = new int[length];
		parentIds = (int[]) ois.readObject();
		ois.close();
	}
	
	public void load(Configuration conf, String fileName) throws IOException, ClassNotFoundException {
		FileSystem fs = FileSystem.get(conf);
		ObjectInputStream ois = new ObjectInputStream(fs.open(new Path(fileName)));
		int length = ois.readInt();
		parentIds = new int[length];
		parentIds = (int[]) ois.readObject();
		ois.close();
	}

	@Override
	public int getParent(int item){
		return parentIds[item];
	}
	
	@Override
	public boolean hasParent(int item){
		//return (parentIds[item] == 0) ? false : true;
		return parentIds[item] > 0;
	}

	@Override
	public int getDepth(int item) { //TODO:
		return 0;
	}

	@Override
	public int getRoot(int item) {
		while(parentIds[item] != 0){
			item = parentIds[item];
		}
		return item;
	}

	@Override
	public int getCommonParent(int item1, int item2) { //TODO:
		return 0;
	}

	public static void main(String args[]) throws Exception{
		@SuppressWarnings("unused")
		Taxonomy t = new NytTaxonomy("data/taxonomy/taxonomy_3.dat");
		System.out.println("***");
	}

	@Override
	public int maxDepth() {
		return maxDepth;
	}

	@Override
	public boolean isParent(int parent, int child) {
		do {
			if ( parent == parentIds[child])
				return true;
			child = parentIds[child];
		} while(child > 0);
		return false;
	}

	@Override
	public boolean isGeneralizationOf(int item1, int item2) { //TODO:
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int[] getParents() {
		return parentIds;
	}

}
