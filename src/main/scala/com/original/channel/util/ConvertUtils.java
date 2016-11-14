package com.original.channel.util;

public class ConvertUtils {
	public static long toLong(Object obj){
		long result=0;
		try {
			if(obj!=null){				
				result=Long.parseLong(obj.toString());
			}
		} catch (NumberFormatException e) {
		}
		return result;
	}

}
