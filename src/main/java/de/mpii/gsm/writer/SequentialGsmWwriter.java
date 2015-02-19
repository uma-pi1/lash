package de.mpii.gsm.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.mahout.math.map.OpenIntObjectHashMap;

public class SequentialGsmWwriter implements GsmWriter {

	private OpenIntObjectHashMap<String> itemIdToItemMap = new OpenIntObjectHashMap<String>();  
	
	private String outputPath;
	
	
  public void setItemIdToItemMap(OpenIntObjectHashMap<String> itemIdToItemMap) {
  	this.itemIdToItemMap = itemIdToItemMap;
  }

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
		
		File outFile = new File(outputPath + "/translatedFS");
		
		File parentFile = outFile.getParentFile();

		outFile.delete();
		parentFile.delete();
		parentFile.mkdirs();
		
	}

	@Override
	public void write(int[] sequence, long count) throws IOException, InterruptedException {

		BufferedWriter br = new BufferedWriter(new FileWriter(outputPath + "/translatedFS", true));

		for (int itemId : sequence)
			br.write(this.itemIdToItemMap.get(itemId) + " ");
		
		br.write("\t" + count + "\n");
		br.close();
	}

}
