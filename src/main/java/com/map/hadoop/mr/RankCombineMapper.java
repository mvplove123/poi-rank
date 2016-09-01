package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * poi rank mapper 合并统计
 */
public class RankCombineMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Logger logger = LoggerFactory.getLogger(RankCombineMapper.class);


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

            if (result.length == 21) { //rank数据统计

                String dataId = result[1];
                String rank = result[20];
                context.write(new Text(dataId), new Text("rank_" + rank));

            } else {//聚类poi数据
                String dataId = result[1];
                String city = result[3];

                context.write(new Text(dataId), new Text("poi_" + str));


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
