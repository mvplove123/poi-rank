package com.map.util;
/*
 * Created on 2006-3-8
 *
 * CoordsysConvertor包用来处理不同坐标系之间的坐标转换问题。
 *
 */

/**
 * @author billkong
 * 
 * 
 *         Convertor_LL_Mer类用来支持经纬度（Longitude/Latitude，以下简称LL）坐标与墨卡托（Mercator，
 *         以下简称Mer ）坐标之间的相互转换。
 * 
 *         基本的思路： 假定存在一个多项式支持LL坐标和Mer坐标之间的转换，即存在X' =
 *         ∑Ai*X^i(i=0,...n)。其中X'为Mer/LL坐标，X为LL/Mer坐标。
 *         用线性回归的方法来寻找系数Ai。从1次多项式，即n=1开始尝试，逐渐加大n直到回归后的标准差小于给定值为止。
 * 
 *         为了保证回归的标准差小于给定值，需要分纬度带分别进行回归。
 *         经Matlab实际检验，纬度带跨度为15度时，LL2Mer或Mer2LL的横坐标需要一个1次多项式
 *         （即线形关系）来拟合即可；而纵坐标则需要一个6次多项式（即n=6）来拟合。
 *         此时中低纬地区（纬度在±60区间以内）的Mer2LL的标准差低于1e-6，LL2Mer的标准差低于10，距离意义上均满足0.1米级精度。
 *         由于Mercator投影的特点
 *         ，越是高纬地区，回归的效果越差。经测试，当纬度超过±85度后，多项式的近似计算结果变得极不可信。考虑到纬度在±
 *         85度以上地区的坐标转换并太大无实际意义（无人居住的南北极圈以内），因此本方法不予支持。
 *         最后实际的纬度带划分为6个：0～15度带；15～30度带；30～45度带；45～60度带；60～75度带；75～85度带。
 *         为了保证调用本模块时返回值的一致性
 *         ，如果外部传进来的纬度参数超过了±85，则将采用75～85度带的系数进行换算。这个换算结果的误差是非常大的，基本上不可用。
 * 
 * 
 *         所有的系数回归均通过第一象限的样本得到，即X>=0且Y>=0的情况，此时代表的是“东经/北纬”地区。
 *         其他地区只需简单地将坐标对称映射到第一象限 ，算出结果后再反向映射回去即可。
 * 
 * 
 *         经纬度下的距离量算公式是将地球近似为一个正球体，半径为赤道半径与地轴半径的均值。经测试此公式在大部分情况下（
 *         最恶劣的情况是当两点的纬度跨度非常大时 ）满足1米级精度。
 * 
 */
public class Convertor_LL_Mer {
	static final double EARTHRADIUS = 6370996.81;
	static final int DIR_LL2MER = 0;
	static final int DIR_MER2LL = 1;

	private Convertor_LL_Mer() {
		// 无需构造函数
	}

	public static double[] LL2Mer(double longitude, double latitude) {
		return ConvertCoord(longitude, latitude, DIR_LL2MER);
	}

	public static double[] Mer2LL(double mercatorX, double mercatorY) {
		return ConvertCoord(mercatorX, mercatorY, DIR_MER2LL);
	}

	public static double[] GetBoundMer(double centerX, double centerY,
			double radius) {
		double[] ret = new double[4];

		double[] temp;
		temp = Mer2LL(centerX, centerY);

		ret = GetBoundLL(temp[0], temp[1], radius);
		temp = LL2Mer(ret[0], ret[1]);
		ret[0] = temp[0];
		ret[1] = temp[1];
		temp = LL2Mer(ret[2], ret[3]);
		ret[2] = temp[0];
		ret[3] = temp[1];

		return ret;
	}

	public static double[] GetBoundLL(double centerX, double centerY,
			double radius) {
		double[] ret = new double[4];

		double r;
		double deltaX;
		double y1;
		double y2;

		// 转换为弧度
		centerX = Math.toRadians(centerX);
		centerY = Math.toRadians(centerY);

		// 理论上给出的距离不能超过地球周长的一半。如果超过则只把超出的部分当作距离
		r = (radius / EARTHRADIUS);
		r = r - (long) r * Math.PI;

		deltaX = Math
				.acos((Math.cos(r) - Math.sin(centerY) * Math.sin(centerY))
						/ (Math.cos(centerY) * Math.cos(centerY)));
		y1 = Math.acos(Math.cos(centerY) * Math.cos(r) + Math.sin(centerY)
				* Math.sin(r));
		y2 = Math.acos(Math.cos(centerY) * Math.cos(r) - Math.sin(centerY)
				* Math.sin(r));

		ret[0] = Math.toDegrees(centerX - deltaX); // lefttopX
		ret[1] = y2 > y1 ? Math.toDegrees(y1) : Math.toDegrees(y2); // lefttopY
		ret[2] = Math.toDegrees(centerX + deltaX); // rightbottomX
		ret[3] = y2 < y1 ? Math.toDegrees(y1) : Math.toDegrees(y2); // rightbottomY

		return ret;
	}

	public static double DistanceLL(double x1, double y1, double x2, double y2) {
		x1 = Math.toRadians(x1);
		y1 = Math.toRadians(y1);
		x2 = Math.toRadians(x2);
		y2 = Math.toRadians(y2);
		return EARTHRADIUS
				* Math.acos((Math.sin(y1) * Math.sin(y2) + Math.cos(y1)
						* Math.cos(y2) * Math.cos(x2 - x1)));
	}

	public static double DistanceMer(double x1, double y1, double x2, double y2) {
		double[] xy1;
		double[] xy2;

		xy1 = Mer2LL(x1, y1);
		x1 = Math.toRadians(xy1[0]);
		y1 = Math.toRadians(xy1[1]);

		xy2 = Mer2LL(x2, y2);
		x2 = Math.toRadians(xy2[0]);
		y2 = Math.toRadians(xy2[1]);

		double acos=(Math.sin(y1) * Math.sin(y2) + Math.cos(y1)
				* Math.cos(y2) * Math.cos(x2 - x1));

				//以下为新加的判断
				if(acos>1.0){
				acos=1.0;
				}
		return EARTHRADIUS
				* Math.acos(acos);
	}

	private static double[] ConvertCoord(double x, double y, int direction) {
		double[] ret = new double[2];

		boolean isEast = true;
		boolean isNorth = true;

		int i;
		double temp;

		double[] band;
		Factor4Band[] factor;
		Factor4Band which;

		// 如果传入的坐标不位于“东经、北纬”的第一象限，将其映射到第一象限
		if (x < 0) {
			x = -1 * x;
			isEast = false;
		}
		if (y < 0) {
			y = -1 * y;
			isNorth = false;
		}

		// 根据转换的方向设定变量
		if (direction == DIR_MER2LL) {
			band = Factor4Band.MERBAND;
			factor = Factor4Band.MER2LL;
		} else {
			band = Factor4Band.LATBAND;
			factor = Factor4Band.LL2MER;
		}

		// 判断传入的坐标位于哪一个度带，以此来决定使用哪一组参数
		i = 0;
		which = factor[0]; // 随便给个初值，否则编译不过去
		while (band[i] != -1) {
			if (y > band[i]) {
				which = factor[i];
				break;
			}
			i++;
		}

		// 计算经纬度坐标
		ret[0] = which.fx0 + which.fx1 * x;

		temp = y / which.fmy;
		ret[1] = which.fy0 + which.fy1 * temp + which.fy2 * temp * temp
				+ which.fy3 * temp * temp * temp + which.fy4 * temp * temp
				* temp * temp + which.fy5 * temp * temp * temp * temp * temp
				+ which.fy6 * temp * temp * temp * temp * temp * temp;

		// 将计算出的经纬度坐标反向映射回所处象限
		if (!isEast) {
			ret[0] = -1 * ret[0];
		}
		if (!isNorth) {
			ret[1] = -1 * ret[1];

		}
		return ret;
	}

	public static void main(String[] args) {
		double[] xy;
		double[] xy1;
		double clon = 110;
		double clat = 35;
		double dlon = 30;
		double dlat = 20;
		double cmerx = 1.2245277224506771E7;
		double cmery = 4139145.655114358;
		double dmerx = 3.339621060627384e+006;
		double dmery = 3.187382622778973e+006;

		double merx1, mery1, merx2, mery2;
		double lon1, lat1, lon2, lat2;
		double dist;

		lon1 = clon + dlon * Math.random();
		lat1 = clat + dlat * Math.random();
		lon2 = clon - dlon * Math.random();
		lat2 = clat - dlat * Math.random();

		merx1 = cmerx + dmerx * Math.random();
		mery1 = cmery + dmery * Math.random();
		merx2 = cmerx - dmerx * Math.random();
		mery2 = cmery - dmery * Math.random();

		dist = 5000000 * Math.random();

		xy = Convertor_LL_Mer.LL2Mer(lon1, lat1);
		xy1 = Convertor_LL_Mer.Mer2LL(xy[0], xy[1]);
		System.out.println("测试LL2Mer");
		System.out.println(lon1 + "," + lat1 + ":" + xy[0] + "," + xy[1]);
		System.out.println(xy1[0] + "," + xy1[1]);
		System.out.println("");

		xy = Convertor_LL_Mer.Mer2LL(merx1, mery1);
		xy1 = Convertor_LL_Mer.LL2Mer(xy[0], xy[1]);
		System.out.println("测试Mer2LL");
		System.out.println(merx1 + "," + mery1 + ":" + xy[0] + "," + xy[1]);
		System.out.println(xy1[0] + "," + xy1[1]);
		System.out.println("");

		xy = GetBoundLL(lon1, lat1, dist);
		System.out.println("测试GetBoundLL");
		System.out.println(lon1 + "," + lat1 + "," + dist + ":");
		System.out.println(xy[0] + "," + xy[1] + "," + xy[2] + "," + xy[3]);
		System.out.println("");

		xy = GetBoundMer(merx1, mery1, dist);
		System.out.println("测试GetBoundMer");
		System.out.println(merx1 + "," + mery1 + "," + dist + ":");
		System.out.println(xy[0] + "," + xy[1] + "," + xy[2] + "," + xy[3]);
		System.out.println("");

		dist = DistanceLL(lon1, lat1, lon2, lat2);
		System.out.println("测试DistanceLL");
		System.out.println(lon1 + "," + lat1 + "," + lon2 + "," + lat2 + " : "
				+ dist);
		System.out.println("");

		dist = DistanceMer(merx1, mery1, merx2, mery2);
		System.out.println("测试DistanceMer");
		System.out.println(merx1 + "," + mery1 + "," + merx2 + "," + mery2
				+ " : " + dist);
		System.out.println("");
		GetBoundMer(12949330, 4837433, 2000);
	}

	public static double[] formXYArray(String xy) {
		String[] xyArray = xy.split(",");
		try {
			int len = xyArray.length;
			double[] coord = new double[len];
			for (int i = 0; i < len;) {
				double x = StringUtil.str2double(xyArray[i]);
				coord[i] = x;
				++i;
				double y = StringUtil.str2double(xyArray[i]);
				coord[i] = y;
				++i;
			}
			return coord;
		} catch (Exception e) {
			// LogUtil.log.info("坐标转换出错！");
		}
		return null;
	}
}

class Factor4Band {
	// 定义纬度带。最后一个-1是为了循环时判断终止条件的方便
	static final double LATBAND_75 = 75;
	static final double LATBAND_60 = 60;
	static final double LATBAND_45 = 45;
	static final double LATBAND_30 = 30;
	static final double LATBAND_15 = 15;
	static final double LATBAND_00 = 0;
	public static final double[] LATBAND = { LATBAND_75, LATBAND_60,
			LATBAND_45, LATBAND_30, LATBAND_15, LATBAND_00, -1 };

	// 定义墨卡托纵坐标带
	static final double MERBAND_75 = 12890594.86;
	static final double MERBAND_60 = 8362377.87;
	static final double MERBAND_45 = 5591021;
	static final double MERBAND_30 = 3481989.83;
	static final double MERBAND_15 = 1678043.12;
	static final double MERBAND_00 = 0;
	public static final double[] MERBAND = { MERBAND_75, MERBAND_60,
			MERBAND_45, MERBAND_30, MERBAND_15, MERBAND_00, -1 };

	// 多项式的系数
	double fx0, fx1, fy0, fy1, fy2, fy3, fy4, fy5, fy6, fmy;

	// 私有构造函数，确保无法创建此类的实例。无论Mer2LL或LL2Mer均使用这个构造函数
	private Factor4Band(double fx0, double fx1, double fy0, double fy1,
			double fy2, double fy3, double fy4, double fy5, double fy6,
			double fmy) {
		this.fx0 = fx0;
		this.fx1 = fx1;
		this.fy0 = fy0;
		this.fy1 = fy1;
		this.fy2 = fy2;
		this.fy3 = fy3;
		this.fy4 = fy4;
		this.fy5 = fy5;
		this.fy6 = fy6;
		this.fmy = fmy;
	}

	// 定义Mer2LL的转换参数
	public static final Factor4Band MER2LL_00 = new Factor4Band(
			2.890871144776878e-009, 8.983055095805407e-006, -0.00000003068298,
			7.47137025468032, -0.00000353937994, -0.02145144861037,
			-0.00001234426596, 0.00010322952773, -0.00000323890364,
			8.260885000000000e+005);

	public static final Factor4Band MER2LL_15 = new Factor4Band(
			3.091913710684370e-009, 8.983055096812155e-006, 0.00006995724062,
			23.10934304144901, -0.00023663490511, -0.63218178102420,
			-0.00663494467273, 0.03430082397953, -0.00466043876332,
			2.555164400000000e+006);

	public static final Factor4Band MER2LL_30 = new Factor4Band(
			-1.981981304930552e-008, 8.983055099779535e-006, 0.03278182852591,
			40.31678527705744, 0.65659298677277, -4.44255534477492,
			0.85341911805263, 0.12923347998204, -0.04625736007561,
			4.482777060000000e+006);

	public static final Factor4Band MER2LL_45 = new Factor4Band(
			-3.030883460898826e-008, 8.983055099835780e-006, 0.30071316287616,
			59.74293618442277, 7.35798407487100, -25.38371002664745,
			13.45380521110908, -3.29883767235584, 0.32710905363475,
			6.856817370000000e+006);

	public static final Factor4Band MER2LL_60 = new Factor4Band(
			-7.435856389565537e-009, 8.983055097726239e-006, -0.78625201886289,
			96.32687599759846, -1.85204757529826, -59.36935905485877,
			47.40033549296737, -16.50741931063887, 2.28786674699375,
			1.026014486000000e+007);

	public static final Factor4Band MER2LL_75 = new Factor4Band(
			1.410526172116255e-008, 8.983055096488720e-006, -1.99398338163310,
			2.009824383106796e+002, -1.872403703815547e+002, 91.60875166698430,
			-23.38765649603339, 2.57121317296198, -0.03801003308653,
			1.733798120000000e+007);

	public static final Factor4Band[] MER2LL = { MER2LL_75, MER2LL_60,
			MER2LL_45, MER2LL_30, MER2LL_15, MER2LL_00 };

	// 定义LL2Mer的转换参数
	public static final Factor4Band LL2MER_00 = new Factor4Band(
			-3.218135878613132e-004, 1.113207020701615e+005, 0.00369383431289,
			8.237256402795718e+005, 0.46104986909093, 2.351343141331292e+003,
			1.58060784298199, 8.77738589078284, 0.37238884252424,
			7.45000000000000);

	public static final Factor4Band LL2MER_15 = new Factor4Band(
			-3.441963504368392e-004, 1.113207020576856e+005,
			2.782353980772752e+002, 2.485758690035394e+006,
			6.070750963243378e+003, 5.482118345352118e+004,
			9.540606633304236e+003, -2.710553267466450e+003,
			1.405483844121726e+003, 22.50000000000000);

	public static final Factor4Band LL2MER_30 = new Factor4Band(
			0.00220636496208, 1.113207020209128e+005, 5.175186112841131e+004,
			3.796837749470245e+006, 9.920137397791013e+005,
			-1.221952217112870e+006, 1.340652697009075e+006,
			-6.209436990984312e+005, 1.444169293806241e+005, 37.50000000000000);

	public static final Factor4Band LL2MER_45 = new Factor4Band(
			0.00337398766765, 1.113207020202162e+005, 4.481351045890365e+006,
			-2.339375119931662e+007, 7.968221547186455e+007,
			-1.159649932797253e+008, 9.723671115602145e+007,
			-4.366194633752821e+007, 8.477230501135234e+006, 52.50000000000000);

	public static final Factor4Band LL2MER_60 = new Factor4Band(
			8.277824516172526e-004, 1.113207020463578e+005,
			6.477955746671608e+008, -4.082003173641316e+009,
			1.077490566351142e+010, -1.517187553151559e+010,
			1.205306533862167e+010, -5.124939663577472e+009,
			9.133119359512032e+008, 67.50000000000000);

	public static final Factor4Band LL2MER_75 = new Factor4Band(
			-0.00157021024440, 1.113207020616939e+005, 1.704480524535203e+015,
			-1.033898737604234e+016, 2.611266785660388e+016,
			-3.514966917665370e+016, 2.659570071840392e+016,
			-1.072501245418824e+016, 1.800819912950474e+015, 82.50000000000000);

	public static final Factor4Band[] LL2MER = { LL2MER_75, LL2MER_60,
			LL2MER_45, LL2MER_30, LL2MER_15, LL2MER_00 };
}
