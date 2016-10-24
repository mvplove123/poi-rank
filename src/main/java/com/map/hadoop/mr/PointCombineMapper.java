package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.model.Poi;
import com.map.util.CategoryThreshold;
import com.map.util.Handle;
import com.map.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * point mapper 统计
 */
public class PointCombineMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(PointCombineMapper.class);


    Map<String,Integer> categoryGps =  CategoryThreshold.getCategoryGps();

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
            String[] result = str.split("\t");

            if (result.length == 2) { //结构化

                String dataId = result[0];
                String point = result[1];
                context.write(new Text(dataId), new Text("structure_"+point));

            } else {//聚类poi数据

                try {       //全量poi统计
                    Poi poi = Handle.parsePoi(str);

                    String name =  poi.name;
                    String dataId = "1_" + poi.dataid;
                    String category = poi.bigclass;
                    String subCategory = poi.smallclass;
                    String city = poi.city;

                    //坐标
                    if (poi.x == 0 || poi.y == 0) {
                        return;
                    }
                    String point = String.valueOf(poi.x) + "," + String.valueOf(poi.y);



                    String[] xy = point.split(",");

                    String cateGpskey = category+"-"+subCategory;
                    Integer dim = categoryGps.get(cateGpskey);

                    CellCut cellCut = new CellCut(50, dim);


                    String boundXY = cellCut.getCrossCellXY(xy[0],xy[1]);
                    Joiner joiner = Joiner.on("\t");
                    String mapValue = joiner.join(new String[]{ name, city, category, subCategory, boundXY});
                    context.write(new Text(dataId), new Text("poi_"+mapValue));


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
    }

}
