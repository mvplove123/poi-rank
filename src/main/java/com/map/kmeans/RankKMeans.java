package com.map.kmeans;

import com.map.util.SelfKMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.util.*;

public class RankKMeans {


    public static final Integer k = 5;

    /**
     * @param args
     */
    public static void main(String[] args) {


        Map<String, Double> oraMap = Maps.newHashMap();

        oraMap.put("beijingshi-1-binGuanFanDian",1.7472072072072073);
        oraMap.put("beijingshi-10-binGuanFanDian",2.808866666666667);
        oraMap.put("beijingshi-11-binGuanFanDian",0.34152909336941817);
        oraMap.put("beijingshi-12-binGuanFanDian",4.464476190476191);
        oraMap.put("beijingshi-13-binGuanFanDian",1.406633318834276);
        oraMap.put("beijingshi-14-binGuanFanDian",0.9762105855855856);
        oraMap.put("beijingshi-15-binGuanFanDian",0.36759550561797755);
        oraMap.put("beijingshi-2-binGuanFanDian",1.2799744897959182);
        oraMap.put("beijingshi-3-binGuanFanDian",0.7842089985486211);
        oraMap.put("beijingshi-4-binGuanFanDian",2.7160893854748602);
        oraMap.put("beijingshi-5-binGuanFanDian",1.711019417475728);
        oraMap.put("beijingshi-6-binGuanFanDian",0.16112404081288473);
        oraMap.put("beijingshi-7-binGuanFanDian",2.110730452674897);
        oraMap.put("beijingshi-8-binGuanFanDian",1.2176068376068379);
        oraMap.put("beijingshi-9-binGuanFanDian",3.715841584158415);



        // TODO Auto-generated method stub
        double[] p = {2.110730, 1.747207, 0.161124, 1.711019,4.464476,  0.341529, 3.715842, 2.808867, 2.716089,
                1.406633, 1.279974,
                1.217607, 0.976211, 0.784209, 0.367596};
        int k = 5;


        Map<String, String> result =  getRankMap(oraMap);

        System.out.println(result);

    }


    public static Map<String, String> getRankMap(Map<String, Double> oraMap) {

        Map<String, String> rankMap = Maps.newHashMap();

        double[] p = new double[oraMap.size()];
        double[][] g;

        int index = 0;
        for (String key : oraMap.keySet()) {
            p[index] = oraMap.get(key);
            index++;
        }


        TreeMap<Double, double[]> sortmap = Maps.newTreeMap();

        Map<Double, Integer> doubleCenterRank = Maps.newHashMap();

        g = cluster(p, k);
        for (int i = 0; i < g.length; i++) {

            Arrays.sort(g[i]);
            double s = g[i][0];
            sortmap.put(s, g[i]);
        }

        int i = 1;
        for (Double s : sortmap.keySet()) {

            double[] values = sortmap.get(s);
            for (double value : values) {
                doubleCenterRank.put(value, i);
            }
            i++;
        }


        for (String key : oraMap.keySet()) {
            Double value = oraMap.get(key);
            Integer rank = doubleCenterRank.get(value);
            rankMap.put(key, String.valueOf(rank));

        }
        return rankMap;
    }


    /*
     * 聚类函数主体。
     * 针对一维 double 数组。指定聚类数目 k。
     * 将数据聚成 k 类。
     */
    public static double[][] cluster(double[] p, int k) {
        // 存放聚类旧的聚类中心
        double[] c = new double[k];
        // 存放新计算的聚类中心
        double[] nc = new double[k];
        // 存放放回结果
        double[][] g;
        // 初始化聚类中心,kmeans++方法是随机选取 k 个
        List<DoublePoint> pointList = new ArrayList<DoublePoint>();
        for (double point : p) {
            DoublePoint doublePoint = new DoublePoint(new double[]{point});
            pointList.add(doublePoint);
        }
        SelfKMeansPlusPlusClusterer kMeansPlusPlusClusterer = new SelfKMeansPlusPlusClusterer(k);
        List<CentroidCluster<Clusterable>> centerResult = kMeansPlusPlusClusterer.chooseInitialCenters(pointList);
        if (!centerResult.isEmpty()) {
            int i = 0;
            for (CentroidCluster<Clusterable> result : centerResult) {
                Clusterable center = result.getCenter();
                for (double pt : center.getPoint()) {
                    c[i] = pt;
                    System.out.println("聚类中心:" + pt);
                }
                i++;
            }
        }


        // 循环聚类，更新聚类中心
        // 到聚类中心不变为止
        while (true) {
            // 根据聚类中心将元素分类
            g = group(p, c);
            // 计算分类后的聚类中心
            for (int i = 0; i < g.length; i++) {
                nc[i] = center(g[i]);
            }
            // 如果聚类中心不同
            if (!equal(nc, c)) {
                // 为下一次聚类准备
                c = nc;
                nc = new double[k];
            } else // 聚类结束
                break;
        }
        // 返回聚类结果
        return g;
    }

    /*
     * 聚类中心函数
     * 简单的一维聚类返回其算数平均值
     * 可扩展
     */
    public static double center(double[] p) {
        return sum(p) / p.length;
    }

    /*
     * 给定 double 型数组 p 和聚类中心 c。
     * 根据 c 将 p 中元素聚类。返回二维数组。
     * 存放各组元素。
     */
    public static double[][] group(double[] p, double[] c) {
        // 中间变量，用来分组标记
        int[] gi = new int[p.length];
        // 考察每一个元素 pi 同聚类中心 cj 的距离
        // pi 与 cj 的距离最小则归为 j 类
        for (int i = 0; i < p.length; i++) {
            // 存放距离
            double[] d = new double[c.length];
            // 计算到每个聚类中心的距离
            for (int j = 0; j < c.length; j++) {
                d[j] = distance(p[i], c[j]);
            }
            // 找出最小距离，返回最小值的下标
            int ci = min(d);
            // 标记属于哪一组
            gi[i] = ci;
        }
        // 存放分组结果
        double[][] g = new double[c.length][];
        // 遍历每个聚类中心，分组
        for (int i = 0; i < c.length; i++) {
            // 中间变量，记录聚类后每一组的大小
            int s = 0;
            // 计算每一组的长度
            for (int j = 0; j < gi.length; j++)
                if (gi[j] == i)
                    s++;
            // 存储每一组的成员
            g[i] = new double[s];
            s = 0;
            // 根据分组标记将各元素归位
            for (int j = 0; j < gi.length; j++)
                if (gi[j] == i) {
                    g[i][s] = p[j];
                    s++;
                }
        }
        // 返回分组结果
        return g;
    }

    /*
     * 计算两个点之间的距离， 这里采用最简单得一维欧氏距离， 可扩展。
     */
    public static double distance(double x, double y) {
        return Math.abs(x - y);
    }

    /*
     * 返回给定 double 数组各元素之和。
     */
    public static double sum(double[] p) {
        double sum = 0.0;
        for (int i = 0; i < p.length; i++)
            sum += p[i];
        return sum;
    }

    /*
     * 给定 double 类型数组，返回最小值得下标。
     */
    public static int min(double[] p) {
        int i = 0;
        double m = p[0];
        for (int j = 1; j < p.length; j++) {
            if (p[j] < m) {
                i = j;
                m = p[j];
            }
        }
        return i;
    }

    /*
     * 判断两个 double 数组是否相等。 长度一样且对应位置值相同返回真。
     */
    public static boolean equal(double[] a, double[] b) {
        if (a.length != b.length)
            return false;
        else {
            for (int i = 0; i < a.length; i++) {
                if (a[i] != b[i])
                    return false;
            }
        }
        return true;
    }
}