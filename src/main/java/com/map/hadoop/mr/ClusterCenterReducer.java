/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: StatReducer.java
 * @Package: com.sogou.map.hadoop.mr
 * @author: huajin.shen
 * @date: 2015年7月16日 上午11:46:09
 * @version: v1.0
 */
package com.map.hadoop.mr;

import com.map.kmeans.Assistance;
import com.map.kmeans.Canopy;
import com.map.model.CalculatePoi;
import com.map.util.CategoryThreshold;
import com.map.util.Cn2Spell;
import com.map.util.FileHandler;
import com.map.util.SelfKMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 聚类中心 计算
 */
public class ClusterCenterReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;

    private final int k = 15;
    Map<String, List<Double>> featureWeight = null;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
        featureWeight = Assistance.getWeitht(context.getConfiguration().get("weightpath"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        List<DoublePoint> pointList = new ArrayList<DoublePoint>();

        String keyStrs = key.toString();
        String keyStr[] = keyStrs.split("\\t");
        String pinyinCity = keyStr[0];
        String cagetoryPy = keyStr[1];

        String outpath = pinyinCity + "/" + cagetoryPy + "/clusterCenter";


        for (Text val : values) {

            String featureValue = val.toString();
            String[] result = featureValue.split("\\t");

            double[] point = new double[]{Double.valueOf(result[6]), Double.valueOf
                    (result[7]), Double.valueOf(result[8]), Double.valueOf(result[9]), Double.valueOf(result[10])
                    , Double.valueOf(result[11]), Double.valueOf(result[12]), Double.valueOf(result[13]), Double
                    .valueOf(result[14]), Double.valueOf(result[15]), Double.valueOf(result[16]), Double.valueOf
                    (result[17])};

            DoublePoint doublePoint = new DoublePoint(point);
            pointList.add(doublePoint);
        }

        SelfKMeansPlusPlusClusterer kMeansPlusPlusClusterer = new SelfKMeansPlusPlusClusterer(k);

        List<CentroidCluster<Clusterable>> centerResult = kMeansPlusPlusClusterer.chooseInitialCenters(pointList);

        if (!centerResult.isEmpty()) {

            int i = 1;
            for (CentroidCluster<Clusterable> result : centerResult) {

                Clusterable center = result.getCenter();
                StringBuilder str = new StringBuilder();
                for (double pt : center.getPoint()) {
                    str.append(pt);
                    str.append("\t");
                }

                //count,centerAvgValue, pinyinCity,label,cagetoryPy
                String outkey = "0" + "\t" + "0" + "\t" + pinyinCity + "\t" + String.valueOf(i) + "\t" + cagetoryPy;
                i++;

                outputs.write(new Text(outkey), new Text(str.toString()), outpath);

            }


        }


    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


    private void export2File(CalculatePoi poi, String matchResult) {

        try {

            Joiner joiner = Joiner.on("\t");
            String logStr = joiner.join(new String[]{poi.getDataId(), poi.getCity(), poi.getPoint(), poi.getCategory(), poi
                    .getSubCategory(), poi.getClassify(),
                    matchResult, poi.getKeyWord()});
            outputs.write(new Text(poi.getName()), new Text(logStr), "realdistance");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {

        SelfKMeansPlusPlusClusterer kMeansPlusPlusClusterer = new SelfKMeansPlusPlusClusterer(15);
        List<DoublePoint> validList = new ArrayList<DoublePoint>();


        String path2 = "D:\\structure\\lvfeature"; //

        try {
            BufferedReader reader1 = FileHandler.getReader(path2, "gb18030");
            String line1 = "";
            while ((line1 = reader1.readLine()) != null) {
                String[] result = line1.split("\t");

                double[] a = new double[]{Double.valueOf(result[5]), Double.valueOf(result[6]), Double.valueOf
                        (result[7]), Double.valueOf(result[8]), Double.valueOf(result[9]), Double.valueOf(result[10])
                        , Double.valueOf(result[11]), Double.valueOf(result[12]), Double.valueOf(result[13]), Double
                        .valueOf(result[14]), Double.valueOf(result[15]), Double.valueOf(result[16])};


                DoublePoint d = new DoublePoint(a);
                validList.add(d);

            }
            reader1.close();


            List<CentroidCluster<Clusterable>> centerresult = kMeansPlusPlusClusterer.chooseInitialCenters(validList);

            for (CentroidCluster<Clusterable> result : centerresult) {

                Clusterable center = result.getCenter();
                center.getPoint().toString();

                StringBuilder str = new StringBuilder();
                for (double a : center.getPoint()) {
                    str.append(a);
                    str.append("\t");
                }
                System.out.println(str.toString());

            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
