/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: StatMapper.java
 * @Package: com.sogou.map.hadoop.mr
 * @author: huajin.shen
 * @date: 2015年7月15日 下午5:11:53
 * @version: v1.0
 */
package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.model.Poi;
import com.map.util.Handle;
import com.map.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * poi 解析mapper
 */
public class PoiMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = Util.getGBKString(value);
        try {

            if(str.isEmpty()){
                return;
            }

            Poi poi = Handle.parsePoi(str);

            String name= poi.name;
            String dataId="1_"+poi.dataid;
            String category = poi.bigclass;
            String subCategory = poi.smallclass;
            String city=poi.city;
            String keyword = poi.keyword;
            String tag= poi.tag;
            String realKeyword;

            String guid=poi.guid;

            if(StringUtils.isNotBlank(keyword) && StringUtils.isNotBlank(tag)){
                realKeyword = keyword+","+tag;
            }else if(StringUtils.isBlank(keyword) && StringUtils.isNotBlank(tag)){
                realKeyword = tag;

            }else if(StringUtils.isNotBlank(keyword) && StringUtils.isBlank(tag)){
                realKeyword = keyword;
            }else{
                realKeyword=" ";
            }


            String price ="0";
            String grade="0";
            String commentNum="0";

            if(category.equals("宾馆饭店")){
                price=poi.minprice;
                grade=poi.hotelrank;
                commentNum=poi.hotelcommentnum;
            }else if(category.equals("旅游景点")){
                price=poi.price;
                grade=poi.praise;
                commentNum=poi.commentcount;
            }else if(category.equals("房地产")){
                price=poi.sellingPrice;
                grade=poi.scoremap;
                commentNum=poi.recordCount;
            }else{
                price=poi.avgPrice;
                grade=poi.scoremap;
                commentNum=poi.recordCount;
            }


            String brand = getBrand(realKeyword);//获取品牌

            //坐标
            if(poi.x == 0 || poi.y ==0){
                return;
            }
            String point = String.valueOf(poi.x) + "," + String.valueOf(poi.y);

            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{"poi",name, dataId,point,city,category,subCategory,
                    brand,realKeyword, commentNum,price,grade,guid});
            context.write(NullWritable.get(),new Text(mapValue));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 提取品牌
     *
     * @param keyword
     * @return
     */
    private static String getBrand(String keyword) {

        if (StringUtils.isBlank(keyword)) {
            return " ";
        }

        String tagBrand = ".*LS:(.*)";//用户id
        Pattern pTagBrand = Pattern.compile(tagBrand);
        Matcher mTagBrand = pTagBrand.matcher(keyword);
        if (mTagBrand.matches()) {
            String currentBrand = mTagBrand.group(1);

            int end = currentBrand.indexOf("$");
            String brand = currentBrand.substring(0, end);
            return brand;
        } else {
            return " ";
        }

    }
}
