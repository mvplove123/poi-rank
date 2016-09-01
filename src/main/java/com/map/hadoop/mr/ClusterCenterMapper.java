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


import com.map.util.CategoryCenter;
import com.map.util.CategoryThreshold;
import com.map.util.Cn2Spell;
import com.map.util.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * 聚类中心
 */
public class ClusterCenterMapper extends Mapper<LongWritable, Text, Text, Text> {



    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);


    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = Util.getGBKString(value);
        try {

            String[] result = str.split("\\t");

            String city = result[2];

            String category = result[3];

            //特殊城市名处理
            Map<String, String> specialCity = CategoryThreshold.getSpecialCity();


            String pinyinCity;
            if (specialCity.get(city) == null) {
                pinyinCity = Cn2Spell.converterToSpell(city);
            } else {
                pinyinCity = specialCity.get(city);
            }

            //获取分类拼音
            Map<String, String> categoryPy = CategoryCenter.getCategoryPY();

            String cagetoryPy = categoryPy.get(category);

            String outKey = pinyinCity+"\t"+cagetoryPy;

            context.write(new Text(outKey), new Text(str));


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
