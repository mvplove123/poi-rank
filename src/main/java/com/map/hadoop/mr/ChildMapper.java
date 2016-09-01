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


import com.map.model.Poi;
import com.map.util.Handle;
import com.map.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 子poi 统计
 */
public class ChildMapper extends Mapper<LongWritable, Text, Text, Text> {

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



        if(result.length == 4){

            String [] structure = str.split("\t");

            context.write(new Text(structure[2]), new Text("guid_"+structure[1]));

        }else{

            try {
                Poi poi = Handle.parsePoi(str);
                String subCategory = poi.smallclass;
                double x = poi.x;
                double y = poi.y;

                String xy=x+","+y;
                String dataId="1_"+poi.dataid;
                context.write(new Text(poi.guid),new Text("name_"+poi.name+"\t"+subCategory+"\t"+xy+"\t"+dataId));

            } catch (Exception e) {
                e.printStackTrace();
            }


        }




    }

}
