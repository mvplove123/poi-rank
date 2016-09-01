package com.map.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTime {
	public static final String DEFAULT_STYLE = "yyyy-MM-dd HH:mm:ss";
	public static final String STYLE0 = "yyyyMMddHHmmss";
	public static final String STYLE1 = "yyyy-MM-dd";
	public static final String STYLE2 = "yyyyMMdd";
	public static String format(long date, String style) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(style);
		return (simpleDateFormat.format(new Date(date)));
	}
	public static String format(long date) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_STYLE);
		return (simpleDateFormat.format(new Date(date)));
	}
	public static long parse(String date) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_STYLE);	
		long ret=0;
		try {
			ret=simpleDateFormat.parse(date).getTime();
		} catch (ParseException e) {
			// TODO 自动生成 catch 块
			//e.printStackTrace();
		}
		return ret;
	}
	public static long parse(String date,String style) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(style);	
		long ret=0;
		try {
			ret=simpleDateFormat.parse(date).getTime();
		} catch (ParseException e) {
			// TODO 自动生成 catch 块
			//e.printStackTrace();
		}
		return ret;
	}
	public static void main(String[] args) {	
		System.out.println("2011-03-0300:00:00");
		long t=DateTime.parse("2011-03-0300:00:00");
		System.out.println(DateTime.format(t));
		
	}
}
