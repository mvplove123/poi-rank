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


import com.map.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;

/**
 * 结构化 统计
 */
public class AbnormalMapper extends Mapper<LongWritable, Text, Text, Text> {

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



        //获取文件名
        InputSplit inputSplit = context.getInputSplit();
        String fileName = ((FileSplit) inputSplit).getPath().getName();

        //重要数据
        if (fileName.startsWith("structureParentPoi")) {//加标记

            String name = result[0];
            String city = result[1];
            context.write(new Text(city + "_" + name), new Text(str));
        } else {//非重要数据


            String name = result[2];
            String city = result[3];
            String category = result[5];
            String subCategory = result[6];

            Joiner joiner = Joiner.on("\t");

            String logStr = joiner.join(new String[]{category, subCategory,
            });
            context.write(new Text(city + "_" + name), new Text("poi" + logStr));
        }




    }

}
