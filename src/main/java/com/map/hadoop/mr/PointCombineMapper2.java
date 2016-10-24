package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.model.Poi;
import com.map.util.CategoryThreshold;
import com.map.util.Handle;
import com.map.util.Util;
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
public class PointCombineMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(com.map.hadoop.mr.PointCombineMapper2.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx)
            throws IOException, InterruptedException {

        String xmlstr = Util.getGBKString(value);
        try {
            Poi poi = Handle.parsePoi(xmlstr);

            String dataId =  "1_" + poi.dataid;

            String[] boundsList = poi.geometry.substring(10,poi.geometry.length()-2).split(",");

            List<String> xlist = Lists.newArrayList();
            List<String> ylist = Lists.newArrayList();

            for(String bounds:boundsList){

                String[] xylist = bounds.split(" ");
                xlist.add(xylist[0]);
                ylist.add(xylist[1]);
            }

            String boundXY = CellCut.getBoundXY(xlist, ylist);
            ctx.write(new Text(dataId), new Text("area_"+boundXY));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
