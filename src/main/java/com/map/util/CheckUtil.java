/**
 * Copyright (C), 2010-2011, Beijing Sogo Co., Ltd.
 * 文件名:     CheckUtil.java
 */
package com.map.util;

/**
 * <p>
 * 功能描述: 线路数据检测公用类
 * </p>
 * <p>
 * 主要方法:
 * <li></li>
 * <li></li>
 * </p>
 * 
 * @author lmr
 * @version 1.0
 * @date Sep 7, 2010
 * @since
 * @see <p>
 *      其它：
 *      </p>
 *      <p>
 *      修改历史：
 *      </p>
 * 
 */

public class CheckUtil {

	// 根据线路经纬度坐标字符串获得线路长度
	public static double getLen(String[] coords) {
		if (coords == null || coords.length < 4) {
			return 0;
		}
		double[] p = new double[coords.length];
		for (int i = 0; i < coords.length; i++) {
			p[i] = Double.parseDouble(coords[i]);
		}
		double dis = 0;
		double x = 0;
		for (int i = 0; i < (p.length - 3); i += 2) {
			if ((p[i] == p[i + 2] && p[i + 1] == p[i + 3]))
				x = 0;
			else
				x = Convertor_LL_Mer.DistanceMer(p[i], p[i + 1], p[i + 2],p[i + 3]);
			if("NaN".equals(x+"")){
				x=0;
			}
			dis += x;
		}
		return dis;
	}
	
	// 根据线路经纬度坐标字符串获得线路长度
	public static double getLen(double[] p) {
		if (p == null || p.length < 4) {
			return 0;
		}
		
		double dis = 0;
		double x = 0;
		for (int i = 0; i < (p.length - 3); i += 2) {
			if ((p[i] == p[i + 2] && p[i + 1] == p[i + 3]))
				x = 0;
			else
				x = Convertor_LL_Mer.DistanceMer(p[i], p[i + 1], p[i + 2],p[i + 3]);
			if("NaN".equals(x+"")){
				x=0;
			}
			dis += x;
		}
		return dis;
	}

	public static double getLen(String coords) {
		if (coords == null) {
			return 0;
		}
		String[] coordArray = coords.split(",");
		return getLen(coordArray);
	}

	// 求叉乘
	public static double Multiply(double x1, double y1, double x2, double y2,
			double x0, double y0) {
		return ((x1 - x0) * (y2 - y0) - (x2 - x0) * (y1 - y0));
	}

	/*
	 * 判断点是否在线上
	 */
	public static boolean IsOnline(double x, double y, double x1, double y1,
			double x2, double y2) {
		return (((x - x1) * (x - x2) <= 0) && ((y - y1) * (y - y2) <= 0) && (Math
				.abs(Multiply(x1, y1, x2, y2, x, y)) < 0.05));
	}

	// 获得两点间的直线距离
	public static double GetDist(double x1, double y1, double x2, double y2) {
		return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
	}

	public static void main(String[] args) {

		String ss="1.29590226998627E7,4842045.62288918,1.29608910174693E7,4844246.00170859";
		double result = getLen(ss);
		System.out.println(result);
	}
}
