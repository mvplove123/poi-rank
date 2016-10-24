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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * brand rank
 */
public class BrandRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    List<String> citys = Lists.newArrayList("北京市","上海市","广州市","深圳市");

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = Util.getGBKString(value);

        String[] result = str.split("\\t");

        String city= result[2];

//        if(citys.contains(city)){
//        }
            //品牌作为key
            String brand = result[5];
            if (StringUtils.isNotBlank(brand)) {
//                String category = result[3];
//                String outkey = brand + "\t" + category;
                context.write(new Text(brand), new Text(str));
            }




    }

}
