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

import com.map.main.KMeansMain;
import com.map.model.CalculatePoi;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 特征值数值化
 */
public class FeatureValueReducer extends Reducer<Text, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(FeatureValueReducer.class);


    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        String mapkey = key.toString();
        String[] keys = mapkey.split("\\t");

        String pinyinCity = keys[0];
        String cagetoryPy = keys[1];

        String path = pinyinCity + "-" + cagetoryPy + "-featurePoi";

        logger.info(path);

        for (Text val : values) {
            String valstr = val.toString();
            outputs.write(NullWritable.get(), new Text(valstr), path);
        }
    }


    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        outputs.close();
    }
}
