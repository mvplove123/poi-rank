package com.map.hadoop.mr;


import com.map.model.CellCut;
import com.map.util.Convertor_LL_Mer;
import com.map.util.Util;
import com.sogou.map.loclog.LocLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * poi Gps mapper 统计
 */
public class GpsMapper extends Mapper<LongWritable, Text, Text, Text> {
    CellCut cellCut = new CellCut(50,50);/////格子范围，实际取值范围应设置在50范围内

    protected static final Logger logger = LoggerFactory.getLogger(GpsMapper.class);


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

            long begin = System.currentTimeMillis();
            //日志读取
            if (!(value.find("\"gpses\":[") == -1 || value.find("\"gpses\":[]") != -1)) {
                LocLogs loclogs = LocLogs.parse(value.toString());
                if (loclogs != null) {
                    for (LocLogs.MatchedItem mi : loclogs.getMatchedItems(LocLogs.MatchedFilter.GPS_ASIDE)) {
                        if (mi.hasGps()) {
                            double lat = mi.getLocation().getLatitude();
                            double lng = mi.getLocation().getLongitude();

                            double[] ll = Convertor_LL_Mer.LL2Mer(lng, lat);






                            //筛选北京坐标数据
                            if (ll[0] >= minX && ll[0] <= maxX && ll[1] >= minY && ll[1] <= maxY) {
                                String x = String.valueOf(ll[0]);
                                String y = String.valueOf(ll[1]);

                                String point = x + "," + y;

                                String currentID = cellCut.getCurrentCell(x, y);
                                context.write(new Text(currentID), new Text(point));
                            }
                        }
                    }
                }
            }




        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
