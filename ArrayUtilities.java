package com.ascend.connect.sqlserver.utils;

public class ArrayUtilities {
	
	public static int addElements (int[] inputArray) {
		int sum =0;
		for( int num : inputArray) {
	          sum = sum+num;
	         }
		return sum;
	
	}

}
