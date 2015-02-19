package de.mpii.gsm.utils;

import java.util.Comparator;

public class MyComparator implements Comparator<Long>{

	@Override
	public int compare(Long o1, Long o2) {
		
		return PrimitiveUtils.getLeft(o1) - PrimitiveUtils.getLeft(o2);// ? 1 : 0;
		
	}

}
