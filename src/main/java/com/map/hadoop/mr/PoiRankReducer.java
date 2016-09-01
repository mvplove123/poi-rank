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

import com.map.model.CalculatePoi;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * poi rank 最终分类映射
 */
public class PoiRankReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {
        countReduce(key,values,ctx);
    }


    private void countReduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        String percentRank="";

        List<String> ranklist = Lists.newArrayList();

        for (Text val : values) {

            String valstr = val.toString();

            if (valstr.startsWith("rank_")) {
                percentRank = valstr.substring(5);
            }
            if (valstr.startsWith("cluster_")) {

                String rank = valstr.substring(8);
                ranklist.add(rank);
            }
        }

        if (StringUtils.isNotEmpty(percentRank) && !ranklist.isEmpty()) {

            for(String rank : ranklist){
                outputs.write(new Text(rank), new Text(percentRank), "rank");
            }
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();
    }

}
