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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * combine 计算
 */
public class CombineReducer extends Reducer<Text, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(CombineReducer.class);

    MultipleOutputs outputs = null;
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        combine(key, values, ctx);

    }


    /**
     * 合并数据
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void combine(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        //引用次数
        String matchCount = "0";
        //结构化数据
        String area = "0";
        String childNum = "0";
        String doorNum = "0";
        String parkNum = "0";
        String internalSceneryNum = "0";
        String buildNum = "0";
        String poiValue = "";


        //热点人气数据
        String hotCountValue = "0";
        //点击量，搜索量
        String searchCountValue = "0\t0\t0";


        for (Text val : values) {
            String valstr = val.toString();

            if (valstr.startsWith("poi_")) {//poi
                poiValue = valstr.substring(4);

            } else if (valstr.startsWith("real_")) {//引用次数
                String realValue = valstr.substring(5);
                matchCount = realValue;

            } else if (valstr.startsWith("search_")){//搜索次数
                String realValue = valstr.substring(7);
                searchCountValue = realValue;

            }else if (valstr.startsWith("hot_")){//热度
                String realValue  = valstr.substring(4);
                hotCountValue = realValue;
            }else if (valstr.startsWith("structure_")){//结构化
                String strutureValue = valstr.substring(10);

                logger.info(strutureValue);

                String[] fields = strutureValue.split("\t");
                if(fields.length!=6){
                    return;
                }
                area = fields[0];
                childNum = fields[1];
                doorNum = fields[2];
                parkNum = fields[3];
                internalSceneryNum = fields[4];
                buildNum = fields[5];
            }
        }


        Joiner joiner = Joiner.on("\t");

        //引用次数，面积，子节点数，门数，停车场数，内部景点数量，建筑数量,人气值,点击量,搜索量
        if (StringUtils.isNotBlank(poiValue)) {
            String newrealValue = poiValue + "\t" + joiner.join(new String[]{matchCount, area, childNum, doorNum,
                    parkNum,
                    internalSceneryNum, buildNum,hotCountValue,searchCountValue});
            outputs.write(NullWritable.get(), new Text(newrealValue), "combine");
        }

    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


}
