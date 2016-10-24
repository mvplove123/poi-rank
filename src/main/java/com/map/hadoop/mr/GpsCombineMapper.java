package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.model.Poi;
import com.map.util.CategoryThreshold;
import com.map.util.Convertor_LL_Mer;
import com.map.util.Handle;
import com.map.util.Util;
import com.sogou.map.loclog.LocLogs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * poi Gps mapper 统计
 */
public class GpsCombineMapper extends Mapper<LongWritable, Text, Text, Text> {
    CellCut cellCut = new CellCut(50, 50);/////格子范围，实际取值范围应设置在50范围内

    protected static final Logger logger = LoggerFactory.getLogger(GpsCombineMapper.class);


    Map<String, Integer> categoryGps = CategoryThreshold.getCategoryGps();

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

        if (result.length == 2) { //gps次数数据统计

            String point = result[0];
            String count = result[1];

            String[] xy = point.split(",");
            String currentID = cellCut.getCurrentCell(xy[0], xy[1]);
            context.write(new Text(currentID), new Text("gpsCount_" + count));

        }else{
            String dataId = result[0];
            String name=result[1];
            String point = result[2];

            String[] xy = point.split(",");
            List<String> crossCells = cellCut.getCrossCell(Double.valueOf(xy[0]),Double.valueOf(xy[1]),Double.valueOf
                    (xy[2]), Double.valueOf(xy[3]));
            for(String crossCell : crossCells){
                context.write(new Text(crossCell), new Text("poi_" + dataId+"\t"+name));

            }

        }






    }

}
