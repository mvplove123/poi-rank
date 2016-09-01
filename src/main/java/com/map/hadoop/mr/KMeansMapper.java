package com.map.hadoop.mr;

import com.map.kmeans.Assistance;
import com.map.util.CategoryCenter;
import com.map.util.CategoryThreshold;
import com.map.util.Cn2Spell;
import com.map.util.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(KMeansMapper.class);

    Map<String, List<Double>> featureWeight = null;
    Multimap<String, Map<String, List<Double>>> cateCenters = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        cateCenters = Assistance.getCenters(context.getConfiguration().get("centerpath"));
        featureWeight = Assistance.getWeitht(context.getConfiguration().get("weightpath"));

        super.setup(context);
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = Util.getGBKString(value);
        String[] fields = line.split("\\t");
        double minDist = Double.MAX_VALUE;
        //加权特征值总和
        double sumWeightValue=0;

        String centerIndex = "1";


        //获取城市信息
        String city = fields[2];

        Map<String, String> specialCity = CategoryThreshold.getSpecialCity();

        //城市拼音
        String pinyinCity;
        if (specialCity.get(city) == null) {
            pinyinCity = Cn2Spell.converterToSpell(city);
        } else {
            pinyinCity = specialCity.get(city);
        }



        //获取大类信息
        String category = fields[3];
        Map<String, String> categoryPy = CategoryCenter.getCategoryPY();

        String cagetoryPy = categoryPy.get(category);


        //获取分类聚类中心

        String cityCategory = pinyinCity+"-"+cagetoryPy;
        Collection<Map<String, List<Double>>> catelabelFeature =  cateCenters.get(cityCategory);

        //获取分类特征权值
        List<Double> weight = featureWeight.get(category);


        if(catelabelFeature.isEmpty()){
            logger.info("聚类中心为 null");

        }

        if(weight.isEmpty()){
            logger.info("weight null");
        }

        if(catelabelFeature.isEmpty() || weight.isEmpty()){
            return;
        }

        logger.info("map:"+catelabelFeature);




        if(!catelabelFeature.isEmpty() && !weight.isEmpty()){

            //计算样本点到各个中心的距离，并把样本聚类到距离最近的中心点所属的类

            for(Iterator it = catelabelFeature.iterator(); it.hasNext();) {
                Map<String, List<Double>> labelFeature = (Map<String, List<Double>>) it.next();

                for (String lableKey : labelFeature.keySet()) {

                    List<Double> featureValue = labelFeature.get(lableKey);
                    double currentDist = 0;
                    for (int i = 0; i < featureValue.size(); i++) {
                        double tmp = Math.abs(Double.parseDouble(fields[i + 6]) - featureValue.get(i));
                        currentDist += Math.pow(weight.get(i),2)* Math.pow(tmp, 2);

                    }

                    double distance = Math.sqrt(currentDist);

                    if (minDist > distance) {
                        minDist = distance;
                        centerIndex = lableKey;
                    }
                }


            }

            DecimalFormat df   =new DecimalFormat("#.0000");

            for(int j=0;j<weight.size();j++){
                sumWeightValue += Double.parseDouble(fields[j + 6])*weight.get(j);
            }

            System.out.println("label"+centerIndex+"\t"+line);

            String outValue = line+"\t"+df.format(sumWeightValue);


            context.write(new Text(pinyinCity+"\t"+centerIndex+"\t"+cagetoryPy), new Text(outValue));

        }

    }

    public static void main(String[] args) {
        Multimap<String, Map<String, List<Double>>> oldcenters = ArrayListMultimap.create();
        Multimap<String, Map<String, List<Double>>> newcenters = ArrayListMultimap.create();


        List<Double> a  = Lists.newArrayList(1d,0d,3d);
        List<Double> b  = Lists.newArrayList(2d,5d,3d);

        Map<String, List<Double>> map1 = Maps.newHashMap();
        Map<String, List<Double>> map2 = Maps.newHashMap();

        map1.put("1",a);
        map2.put("2",b);


        oldcenters.put("lvyou",map1);
        oldcenters.put("lvyou",map2);

        oldcenters.put("chengshi",map1);
        oldcenters.put("chengshi",map2);

        List<Double> c  = Lists.newArrayList(1d,2d,3d);
        List<Double> d  = Lists.newArrayList(4d,5d,6d);

        Map<String, List<Double>> map3 = Maps.newHashMap();
        Map<String, List<Double>> map4 = Maps.newHashMap();

        map3.put("1",c);
        map4.put("2",d);
        newcenters.put("lvyou",map3);
        newcenters.put("lvyou",map4);

        newcenters.put("chengshi",map3);
        newcenters.put("chengshi",map4);



        float threshold = 0.0001f;


        for (String cateKey : oldcenters.keySet()) {

            Collection<Map<String, List<Double>>> oldlabelmap =  oldcenters.get(cateKey);
            Collection<Map<String, List<Double>>> newlabelmap =  newcenters.get(cateKey);
            System.out.println("okey:"+cateKey+"old data"+oldlabelmap);
            System.out.println("nkey"+cateKey+"new data"+newlabelmap);
            float distance = 0;

            for(Iterator oit = oldlabelmap.iterator(); oit.hasNext();) {

                Map<String, List<Double>> oldlabelFeature = (Map<String, List<Double>>) oit.next();


                for(Iterator nit = newlabelmap.iterator(); nit.hasNext();){
                    Map<String, List<Double>> newlabelFeature = (Map<String, List<Double>>) nit.next();

                    //计算新旧同类 所有label 差值
                    for (String lableKey : oldlabelFeature.keySet()) {

                        List<Double> oldfeatureValue = oldlabelFeature.get(lableKey);
                        if(newlabelFeature.get(lableKey) == null){
                            break;
                        }

                        List<Double> newfeatureValue = newlabelFeature.get(lableKey);

//                        System.out.println("old:"+oldfeatureValue);
//                        System.out.printf("new:"+newfeatureValue);
                        //计算新旧同类 相同label 差值
                        for (int i = 0; i < oldfeatureValue.size(); i++) {
                            double tmp = Math.abs(oldfeatureValue.get(i) - newfeatureValue.get(i));
                            distance += Math.pow(tmp, 2);
                        }

                    }
                }

            }
            System.out.println("cateKey = " + cateKey + "Distance = " + distance + " Threshold = " + threshold);
            //任何一类不符合阀值，则更新聚类中心
            if (distance > threshold){
                break;
            }
        }

        System.out.println("ok");
    }

}