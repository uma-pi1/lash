package de.mpii.gsm.taxonomy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;



public interface Taxonomy {
	
	public void load(String fileName) throws Exception;
	
	public void load(Configuration conf, String taxonomyURI) throws IOException, ClassNotFoundException;
	
	public void load(int[] p);
	
	/**
	 * @param child Item from the taxonomy
	 * @return parent id
	 */
	public int getParent(int item);
	
	/**
	 * @param child Item from the taxonomy
	 * @return true/false
	 */
	public boolean hasParent(int item);
	
	/**
	 * @param child
	 * @return
	 */
	public int getDepth(int item);
	
	
	/**
	 * @param item
	 * @return
	 */
	public int getRoot(int item);
	
	
	/**
	 * @param item1
	 * @param item2
	 * @return
	 */
	public int getCommonParent(int item1, int item2);
	
	public boolean isParent(int parent, int child);
	
	public boolean isGeneralizationOf(int item1, int item2);


	public int maxDepth();
	
	public int[] getParents();

}
