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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;

/**
 * 父poi 统计
 */
public class ParentMapper extends Mapper<LongWritable, Text, Text, Text> {
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

        String[] result = str.split("\t");



        if(result.length == 5){

            String [] structure = str.split("\t");

            String parentGuid = structure[0];
            String childName = "guid_"+structure[1];
            String childSubCategory = structure[2];
            String xy = structure[3];
            String childDataId = structure[4];


            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{childName,childSubCategory,xy,childDataId});
            context.write(new Text(parentGuid), new Text(mapValue));

        }else{

            try {
                Poi poi = Handle.parsePoi(str);

                String name= "name_"+poi.name;
                String dataId="1_"+poi.dataid;
                String category = poi.bigclass;
                String subCategory = poi.smallclass;
                String city=poi.city;

                Joiner joiner = Joiner.on("\t");
                String mapValue = joiner.join(new String[]{name, dataId,city,category,subCategory});
                context.write(new Text(poi.guid),new Text(mapValue));

            } catch (Exception e) {
                e.printStackTrace();
            }


        }




    }

}
