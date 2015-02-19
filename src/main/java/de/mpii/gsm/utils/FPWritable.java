package de.mpii.gsm.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



/**
 * Writable that represents a value for an item key with item Frequency and Parent information.
 * 
 * @author kbeedkar
 *
 */
public class FPWritable implements WritableComparable<FPWritable>{
	
	private int frequency = 0;
	private String parent = null;
	
	public FPWritable(){
		
	}
	
	public FPWritable(int frequency, String parent) {
		this.frequency = frequency;
		this.parent = parent;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		frequency = WritableUtils.readVInt(di);
		parent = WritableUtils.readString(di);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeVInt(d, frequency);
		WritableUtils.writeString(d, parent);
	}

	@Override
	public int compareTo(FPWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getFrequency(){
		return frequency;
	}
	
	public String getParent(){
		return parent;
	}
	
	public void setFrequency(int frequency){
		this.frequency = frequency;
	}
	
	public void setParent(String parent) {
		this.parent = parent;
	}
	
	
	public static void main(String[] args) {
		
		
		FPWritable w = new FPWritable();
		w.setFrequency(10);
		w.setParent("abcd");
		
		System.out.println(w.getFrequency() + " " + w.getParent());
		
		
		w.setFrequency(12341234);
		w.setParent(null);
		
		System.out.println(w.getFrequency() + " " + w.getParent());
		
		if(w.getParent() == null){
			System.out.println("null value");
		}
		
	}

}
