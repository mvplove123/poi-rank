package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.util.*;
import com.sogou.map.loclog.LocLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * rank  mapper 统计
 */
public class EvaluatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    CellCut cellCut = new CellCut(1000, 50);/////格子范围，实际取值范围应设置在1000范围内

    protected static final Logger logger = LoggerFactory.getLogger(EvaluatingMapper.class);


    //北京边界
    double maxX = 13080200.9;
    double maxY = 4992752.71;

    double minX = 12848336.26;
    double minY = 4757252.11;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {

            String str = Util.getGBKString(value);
            String[] result = str.split("\t");

            String point = result[2];
            String city = result[3];
            String category = result[4];
            String[] xy = point.split(",");

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

            BigDecimal bigDecimalX = new BigDecimal(xy[0]);
            BigDecimal bigDecimalY = new BigDecimal(xy[1]);


            String pointx= bigDecimalX.toString();
            String pointy = bigDecimalY.toString();

            String outValue = str+"\t"+pointx+"\t"+pointy;

            String currentID = cellCut.getCurrentCell(xy[0], xy[1]);
            String outKey = pinyinCity + "-" + cagetoryPy + "-" + currentID;
            context.write(new Text(outKey), new Text(outValue));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
