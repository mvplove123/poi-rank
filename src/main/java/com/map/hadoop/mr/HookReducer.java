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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;
import java.util.Iterator;

/**
 * gps combine dataId合并 reduce
 */
public class HookReducer extends Reducer<Text, Text, NullWritable, Text> {

    MultipleOutputs outputs = null;



    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        Integer totalCount = 0;
        String dataId = "";
        String name = "";
        String category = "";
        String subCategory = "";
        String point = "";
        String city = "";


        for (Text val : values) {

            String poiValue = val.toString();

            String[] result = poiValue.split("\\t");
            dataId = result[0];
            name = result[1];
            city = result[2];
            category = result[3];
            subCategory = result[4];
            point = result[5];
            String count = result[6];
            totalCount += Integer.valueOf(count);
        }
        Joiner joiner = Joiner.on("\t");
        String value = joiner.join(new String[]{dataId, name, city, category, subCategory, point, String.valueOf(totalCount)});

        outputs.write(NullWritable.get(), new Text(value), "poiHotCount");


    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


    public static void main(String[] args) {

        HookReducer reducer = new HookReducer();
        Text key = new Text("");

        Iterable<Text> values = new Iterable<Text>() {
            @Override
            public Iterator<Text> iterator() {
                return null;
            }
        };
        Context ctx = null;


    }


}
