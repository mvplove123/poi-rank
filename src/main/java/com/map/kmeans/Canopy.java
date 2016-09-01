package com.map.kmeans;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.map.model.FeatureRank;
import com.map.util.FileHandler;
import com.map.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

/**
 * Canopy算法 借助canopy算法计算对应的Kmeans中的K值大小
 * 其中对于计算K值来说，canopy算法中的T1没有意义，只用设定T2(T1>T2) 我们这里将T2设置为平均距离
 *
 * @author YD
 */
public class Canopy {
    private List<DoublePoint> points = new ArrayList<DoublePoint>(); // 进行聚类的点
    private static List<List<DoublePoint>> clusters = new ArrayList<List<DoublePoint>>(); // 存储簇
    private static double T2 = -1; // 阈值
    private List<Double> weight = new ArrayList<Double>();

    public Canopy(List<DoublePoint> points, List<Double> weights) {
        for (DoublePoint point : points) {

            // 进行深拷贝
            this.points.add(point);
        }

        for (Double weight : weights) {
            this.weight.add(weight);
        }
    }

    /**
     * 进行聚类，按照Canopy算法进行计算按照Canopy算法进行计算，将所有点进行聚类
     */
    public static void cluster(List<DoublePoint> points, List<Double> weight) {

        int size = points.size();
        double sumT2 = 0;
        int timeK = 0;
        //循环100次
        while (timeK < 10) {

            List<DoublePoint> partPoints = Lists.newArrayList();

            List<Integer> randomNum = Lists.newArrayList();
            int ranI = 0;
            //随机抽取100个样本点，计算T2
            while (ranI < 1000) {

                RandomGenerator randomGenerator = new JDKRandomGenerator();
                int s = randomGenerator.nextInt(size);
                Random random = new Random();
//                int s = random.nextInt(size) % (size + 1);
                if (!randomNum.contains(s)) {
                    partPoints.add(points.get(s));
                    randomNum.add(s);
                    ranI++;
                }
            }

            double tmpT2 = getAverageDistance(partPoints, weight);
            sumT2 += tmpT2;
            timeK++;
        }
        T2 = sumT2 / 10;

        System.out.println(T2);
        while (points.size() != 0) {
            List<DoublePoint> cluster = new ArrayList<DoublePoint>();
            DoublePoint baseDoublePoint = points.get(0); // 基准点
            double[] baseDoublePointValue = baseDoublePoint.getPoint();
            cluster.add(baseDoublePoint);
            points.remove(0);
            int index = 0;
            while (index < points.size()) {

                DoublePoint anotherDoublePoint = points.get(index);
                double[] anotherDoublePointValue = anotherDoublePoint.getPoint();
                double tmpdistance = 0;
                for (int x = 0; x < baseDoublePointValue.length; x++) {
                    double tmp = Math.abs(baseDoublePointValue[x] - anotherDoublePointValue[x]);
                    tmpdistance += Math.pow(weight.get(x), 2) * Math.pow(tmp, 2);

                }

                double distance= Math.sqrt(tmpdistance);
                if (distance <= T2) {
                    cluster.add(anotherDoublePoint);
                    points.remove(index);
                } else {
                    index++;
                }
            }
            clusters.add(cluster);
        }
    }

    /**
     * 得到Cluster的数目
     *
     * @return 数目
     */
    public static int getClusterNumber() {
        return clusters.size();
    }

    /**
     * 得到的中心点(各点相加求平均)
     *
     * @return 返回中心点
     */
    private static double getAverageDistance(List<DoublePoint> points, List<Double> weight) {

        int pointSize = points.size();
        double sum = 0;

        for (int i = 0; i < pointSize; i++) {

            for (int j = 0; j < pointSize; j++) {
                if (i == j)
                    continue;


                double[] pointA = points.get(i).getPoint();
                double[] pointB = points.get(j).getPoint();

                double tmpSum=0;
                for (int x = 0; x < pointA.length; x++) {
                    double tmp = Math.abs(pointA[x] - pointB[x]);
                    tmpSum += Math.pow(weight.get(x), 2) * Math.pow(tmp, 2);
                }
                sum+=Math.sqrt(tmpSum);
            }
        }


        int distanceNumber = pointSize * (pointSize + 1) / 2;
        double T2 = sum / distanceNumber / 2; // 平均距离的一半
        return T2;


//
//        double sum = 0;
//        int pointSize = points.size();
//        for (int i = 0; i < pointSize; i++) {
//            for (int j = 0; j < pointSize; j++) {
//                if (i == j)
//                    continue;
//                DoublePoint pointA = points.get(i);
//                DoublePoint pointB = points.get(j);
//                pointA.getPoint();
//                sum += Math.sqrt((pointA.x - pointB.x) * (pointA.x - pointB.x)
//                        + (pointA.y - pointB.y) * (pointA.y - pointB.y));
//            }
//        }
//        int distanceNumber = pointSize * (pointSize + 1) / 2;
//        double T2 = sum / distanceNumber / 2; // 平均距离的一半
//        return T2;
    }

    /**
     * 获取阈值T2
     *
     * @return 阈值T2
     */
    public double getThreshold() {
        return T2;
    }

    /**
     * 测试9个点，进行操作
     *
     * @param args
     */
    public static void main(String[] args) {


        String path1 = "D:\\structure\\clusterResult"; //
        String weightpath = "D:\\structure\\poi-weight.txt"; //

        List<DoublePoint> pointList = Lists.newArrayList();

        try {
            BufferedReader reader = FileHandler.getReader(path1, "gb18030");
            String line = "";
            String category = "医疗卫生";
            while ((line = reader.readLine()) != null) {

                String[] result = line.split("\\t");
                String currentCategory = result[1];

                if (category.equals(currentCategory)) {

                    double[] point = new double[]{ Double.valueOf
                            (result[7]), Double.valueOf(result[8]), Double.valueOf(result[9]), Double.valueOf(result[10])
                            , Double.valueOf(result[11]), Double.valueOf(result[12]), Double.valueOf(result[13]), Double
                            .valueOf(result[14]), Double.valueOf(result[15]), Double.valueOf(result[16]),Double
                            .valueOf(result[17]), Double.valueOf(result[18])};

                    DoublePoint doublePoint = new DoublePoint(point);
                    pointList.add(doublePoint);

                }


            }
            reader.close();


            BufferedReader weightreader = FileHandler.getReader(weightpath, "gb18030");

            String weightline = "";
            Map<String, List<Double>> cateweight = Maps.newHashMap();

            while ((weightline = weightreader.readLine()) != null) {

                String[] weights = weightline.split("\t");

                String catekey = weights[0];
                List<Double> tmplist = new ArrayList<Double>();

                for (int i = 2; i < weights.length; i++) {
                    if (StringUtils.isNotBlank(weights[i])) {
                        tmplist.add(Double.parseDouble(weights[i]));
                    }
                }
                cateweight.put(catekey, tmplist);

            }
            weightreader.close();


            cluster(pointList, cateweight.get(category));

            int k = getClusterNumber();
            System.out.println("k值:" + k);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}