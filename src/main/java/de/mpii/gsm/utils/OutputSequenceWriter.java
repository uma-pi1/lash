package de.mpii.gsm.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;


public class OutputSequenceWriter {

	FileWriter writer;
	public OutputSequenceWriter(String fileName) throws IOException{
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		writer = new FileWriter(fileName, true);
	}
	
	public void writeSequenceSupport(int[] sequence, int support) throws IOException {
		writer.write(Arrays.toString(sequence) + ":" + support);
	}
	
	public void writeSequence(int[] sequence) throws IOException {
		writer.write(Arrays.toString(sequence) + "\n");
	}
	
	public void close() throws Exception{
		writer.close();
	}
	

	
	
	public static void main(String[] args) throws Exception {
		OutputSequenceWriter writer = new OutputSequenceWriter("data/output/testfile.txt");
		
		int[] sequence = new int[]{1,2,3,4,5};
		
		writer.writeSequence(sequence);
		sequence = new int[]{4,465,3,65};
		writer.writeSequence(sequence);
		writer.close();
		
		writer = new OutputSequenceWriter("data/output/testfile.txt");
		writer.writeSequence(sequence);
		writer.close();
		
	}

}
