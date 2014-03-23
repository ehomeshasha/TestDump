package com.jz100.util;

import java.util.regex.Pattern;

public class CommonUtil {
	public static final Pattern EMPTY = Pattern.compile("\\s+");
	public static final Pattern SHU = Pattern.compile("\\|");
	public static final String NULL = "null";
	public static final String EMPTY_STRING = "";
	public static final String zero = "0";
	public static final String TAB = "\t";
	public static final String COMMA = ",";
	
	public String setEmpty2Zero(String value) {
		if(value == null || value.equals(EMPTY_STRING) || value.equals(NULL)) {
			return zero;
		}
		return value;
	}
}
