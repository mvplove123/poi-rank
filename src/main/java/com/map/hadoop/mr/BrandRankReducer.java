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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * brand rank
 */
public class BrandRankReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        countReduce(key, values, ctx);
    }


    private void countReduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        int count = 0;
        double sum = 0;
        for (Text val : values) {
            String valstr = val.toString();

            String[] fields = valstr.split("\t");

            Double rank = Double.valueOf(fields[20]);
            sum += rank;
            count++;
        }

        DecimalFormat df   =new DecimalFormat("#.00");

        if (count > 0) {
            String brandRank = df.format(sum / count);
            String outValue = String.valueOf(count) + "\t" + brandRank;

            outputs.write(new Text(key), new Text(outValue), "brandRank");

        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();
    }

}
