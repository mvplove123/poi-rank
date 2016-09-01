package com.map.hadoop.mr;

import com.map.kmeans.Assistance;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    Map<String, List<Double>> featureWeight = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        featureWeight = Assistance.getWeitht(context.getConfiguration().get("weightpath"));

    }


    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {


        String keyStr = key.toString();
        String[] keyStrs= keyStr.split("\t");
        String category = keyStrs[2];



        List<List<Double>> assistList = new ArrayList<List<Double>>();

        String tmpResult = "";
        for (Text val : value) {
            String line = val.toString();
            String[] fields = line.split("\\t");

            category=fields[3];
            List<Double> tmpList = new ArrayList<Double>();
            //从特征取值开始
            for (int i = 6; i < fields.length-1; i++) {
                tmpList.add(Double.parseDouble(fields[i]));
            }
            assistList.add(tmpList);
        }


        int count=assistList.size();

        List<Double> weights = featureWeight.get(category);

        double sumWeight = 0;

        //计算新的聚类中心
        for (int i = 0; i < assistList.get(0).size(); i++) {
            double sum = 0;
            for (int j = 0; j < assistList.size(); j++) {
                sum += assistList.get(j).get(i);
            }
            double tmp = sum / assistList.size();


            //求加权平均值
            double weightTmp = weights.get(i) * tmp;
            sumWeight+=weightTmp;


            if (i == 0) {
                tmpResult += tmp;
            } else {
                tmpResult += "\t" + tmp;
            }
        }



        String outKey = count+"\t"+sumWeight+"\t"+keyStr;

        System.out.println("center value:"+tmpResult);

        Text result = new Text(tmpResult);
        context.write(new Text(outKey), result);
    }
}