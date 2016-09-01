/**  
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: StringUtil.java
 * @Package: com.sogou.map.util
 * @author: huajin.shen  
 * @date: 2015年7月7日 下午7:31:50
 * @version: v1.0  
 */
package com.map.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @ClassName: StringUtil
 * @author: huajin.shen	
 * @date: 2015年7月7日 下午7:31:50
 * @version: 1.0	
 */
public class StringUtil {
	
	public static final String stringValueOf(Object str) {
		try { return str.toString().trim();} 
		catch (Exception e) { return "";}
	}
	
	public static final Double doubleValueOf(Object str) {
		try { return Double.valueOf(str.toString().trim());} 
		catch (Exception e) { return 0d;}
	}
	
	public static final double str2double(String str) {
		try {
			double d = Double.parseDouble(str);
			return d;
		} catch (Exception e) {

		}
		return -1d;
	}
	
	// 格式化字符串,中文括号->英文括号,英文->大写英文
	public static final String normalize(String str) {
		if (str==null || str.trim().equals(""))
			return str;
		
		str = str.replace("（", "(");
		str = str.replace("）", ")");
		str = str.replace("，", ",");
		return str.toUpperCase().trim();
	}

	/**
	 * 分隔字符串 
	 * @param separator 分隔符为空时默认逗号
	 * @return: String
	 */
	public static List<String> split(String str, String separator) {
		List<String> ret = new ArrayList<String>();
		if ( separator==null || separator.trim().equals("") ) {
			separator = ",";
		}
		if (str == null || str.trim().equals("")) {
			return ret;
		}
		String[] arr = str.split(separator);
		for (String i : arr) {
			if(!i.trim().equals(""))
				ret.add(i.trim());
		}
		return ret;
	}
	
	/**
	 * 连接字符串数组 
	 * @param list
	 * @param separator 分隔符为空时默认逗号
	 * @return: String
	 */
	public static String join(List<String> list, String separator) {
		String ret = "";
		if ( separator==null || separator.trim().equals("") ) {
			separator = ",";
		}
		if (list!=null && list.size() > 0) {
			for ( String str : list ) {
				ret += str + separator;
			}
		} else {
			return ret;
		}
		if (ret.endsWith(separator)) {
			ret = ret.substring(0,ret.length()-separator.length());
		}
		return ret;
	}
	
	/**
	 * 连接字符串数组 
	 * @param set
	 * @param separator 分隔符为空时默认逗号
	 * @return: String
	 */
	public static String join(Set<String> set, String separator) {
		String ret = "";
		if ( separator==null || separator.trim().equals("") ) {
			separator = ",";
		}
		if (set!=null && set.size() > 0) {
			for ( String str : set ) {
				ret += str + separator;
			}
		} else {
			return ret;
		}
		if (ret.endsWith(separator)) {
			ret = ret.substring(0,ret.length()-separator.length());
		}
		return ret;
	}
	
	public static boolean isNumeric(String str){
		if (  str == null ) return false;
		
	    Pattern pattern = Pattern.compile("[0-9]+");
	    return pattern.matcher(str).matches();
	}







}
