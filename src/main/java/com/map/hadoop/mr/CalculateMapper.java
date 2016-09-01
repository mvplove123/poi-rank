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
import com.map.util.StringUtil;
import com.map.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;
import java.util.List;

/**
 * poi Calculate mapper 统计
 */
public class CalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
    CellCut cellCut = new CellCut(2000,6000);/////格子范围，实际取值范围应设置在5公里范围内


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
        try {

            Poi poi = Handle.parsePoi(str);

            if(poi.x == 0 || poi.y ==0){
                return;
            }

            String dataId = "1_" + poi.dataid;
            String name = poi.name;
            String category = poi.bigclass;
            String subCategory = poi.smallclass;
            String city = poi.city;
            String point = String.valueOf(poi.x) + "," + String.valueOf(poi.y);
            String alias = poi.alias;
            if(StringUtils.isBlank(alias)){
                alias=" ";
            }



            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{ dataId,name,city,point,category,subCategory,alias});

            String x = String.valueOf(poi.x);
            String y = String.valueOf(poi.y);

            List<String> currentIDs = cellCut.getCrossCell(x, y);
            for (String id : currentIDs) {
                context.write(new Text(id), new Text(mapValue));
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
