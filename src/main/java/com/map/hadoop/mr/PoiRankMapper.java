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


import com.map.kmeans.Assistance;
import com.map.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * poi rank 最终分类映射
 */
public class PoiRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, String> cateCenterRank = null;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        cateCenterRank = Assistance.getCentersRankMap(context.getConfiguration().get("centerpath"));

        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = Util.getGBKString(value);

        String[] result = str.split("\\t");

        //聚类结果
        if (result.length == 22) {

            String cityPy = result[0];
            String label = result[1];
            String categoryPy = result[2];

            String outKey = cityPy + "-" + label + "-" + categoryPy;

            String rankPercent = cateCenterRank.get(outKey);

            context.write(new Text(rankPercent), new Text(str));

        }
    }

}
