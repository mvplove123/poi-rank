package com.map.util;

import com.map.kmeans.Assistance;
import com.map.model.FeatureThreshold;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by admin on 2016/6/13.
 */
public class CategoryCenter {


    public static Map<String, String> getCategoryPY(){
        Map<String, String> cateMap = Maps.newHashMap();
        cateMap.put("旅游景点","lvYouJingDian");
        cateMap.put("宾馆饭店","binGuanFanDian");
        cateMap.put("医疗卫生","yiLiaoWeiSheng");
        cateMap.put("房地产","fangDiChan");
        cateMap.put("学校科研","xueXiaoKeYan");
        cateMap.put("餐饮服务","canYinFuWu");
        cateMap.put("休闲娱乐","xiuXianYuLe");
        cateMap.put("金融银行","jinRongYinHang");
        cateMap.put("场馆会所","changGuanHuiSuo");
        cateMap.put( "公司企业","gongSiQiYe");
        cateMap.put( "邮政电信","youZhengDianXin");
        cateMap.put("政府机关","zhengFuJiGuan");
        cateMap.put("汽车服务","qiCheFuWu");
        cateMap.put("购物场所","gouWuChangSuo");
        cateMap.put( "交通出行","jiaoTongChuXing");
        cateMap.put("地名","diMing");
        cateMap.put("新闻媒体","xinWenMeiTi");
        cateMap.put("体育场馆","tiYuChangGuan" );
        cateMap.put( "其它","qiTa");

        return cateMap;

    }



    public static void main(String[] args) {

        Map<String, String> cateMap = Maps.newHashMap();
        cateMap.put("lvYouJingDian", "旅游景点");
        cateMap.put("binGuanFanDian", "宾馆饭店");
        cateMap.put("yiLiaoWeiSheng", "医疗卫生");
        cateMap.put("fangDiChan", "房地产");
        cateMap.put("xueXiaoKeYan", "学校科研");
        cateMap.put("canYinFuWu", "餐饮服务");
        cateMap.put("xiuXianYuLe", "休闲娱乐");
        cateMap.put("jinRongYinHang", "金融银行");
        cateMap.put("changGuanHuiSuo", "场馆会所");
        cateMap.put("gongSiQiYe", "公司企业");
        cateMap.put("youZhengDianXin", "邮政电信");
        cateMap.put("zhengFuJiGuan", "政府机关");
        cateMap.put("qiCheFuWu", "汽车服务");
        cateMap.put("gouWuChangSuo", "购物场所");
        cateMap.put("jiaoTongChuXing", "交通出行");
        cateMap.put("diMing", "地名");
        cateMap.put("xinWenMeiTi", "新闻媒体");
        cateMap.put("tiYuChangGuan", "体育场馆");
        cateMap.put("qiTa", "其它");


        Map<String, String[]> labelmap = Maps.newHashMap();

        labelmap.put("1", new String[]{"1", "0", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0"});

        labelmap.put("2", new String[]{"2", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"});

        labelmap.put("3", new String[]{"4", "3", "4", "0", "0", "0", "0", "0", "0", "0", "0", "0"});

        labelmap.put("4", new String[]{"5", "4", "2", "0", "0", "0", "0", "0", "0", "0", "0", "0"});

        labelmap.put("5", new String[]{"5", "4", "3", "0", "0", "0", "0", "0", "0", "0", "0", "0"});


        String path1 = "D:\\structure\\cateCenter\\lvYouJingDian";
        String path2 = "D:\\structure\\poi-weight.txt";



        try {
            //zhongxindian
            BufferedReader reader = FileHandler.getReader(path1, "gb18030");
            BufferedReader weightreader = FileHandler.getReader(path2, "gb18030");


            String weightline = "";
            Map<String, List<Float>> weight = Maps.newHashMap();
            String category;
            while ((weightline = weightreader.readLine()) != null) {


                String[] weights = weightline.split("\t");
                category=weights[0];


                List<Float> weightValue = Lists.newArrayList();

                for(int i=2;i<weights.length;i++){
                    weightValue.add(Float.parseFloat(weights[i]));
                }
                weight.put(category,weightValue);

            }
            weightreader.close();


            TreeMap<Float,String> map  = Maps.newTreeMap();
            String line = "";
            while ((line = reader.readLine()) != null) {

                String[] clusterCenter = line.split("\t");

                String categoryCenter = clusterCenter[1];


                List<Float> weightValue =  weight.get(categoryCenter);

                float sum=0;
                for (int i = 0; i < weightValue.size(); i++) {
                    float tmp = weightValue.get(i) * Float.parseFloat(clusterCenter[i+2]);
                    sum+=tmp;
                }

                float meanCenter = sum/weightValue.size();

                map.put(sum,line);

            }
            reader.close();

            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream
                    ("D:\\structure\\cateCenter\\finalCateCenter",
                            false), "GB18030");
            for(Float key : map.keySet()){

                String value = map.get(key);

                writer.write(String.valueOf(key)+"\t"+value+"\n");


            }

            writer.flush();
            writer.close();




            System.out.println("ok");


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
