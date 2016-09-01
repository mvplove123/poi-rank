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
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * 结构化 统计
 */
public class StructureMapper extends Mapper<LongWritable, Text, Text, Text> {




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



        if(result.length == 3){

            String [] structure = str.split("\t");

            String parentId = structure[0];
            String childIds = structure[2];

            for(String dataId : childIds.split(",")){
                context.write(new Text(dataId), new Text("childId_"));
            }
            context.write(new Text(parentId), new Text("parentId_"));

        }else{

            String [] poi = str.split("\t");
            String dataId = poi[0];

            context.write(new Text(dataId), new Text("poi_"+str));
        }




    }

}
