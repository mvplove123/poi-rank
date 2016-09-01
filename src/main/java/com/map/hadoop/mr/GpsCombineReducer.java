package com.map.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * poi Gps reducer 计算
 */
public class GpsCombineReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;
    protected static final Logger logger = LoggerFactory.getLogger(GpsCombineReducer.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        List<String> poiList = Lists.newArrayList();

        int gpsSumCount = 0;
        for (Text val : values) {

            String valstr = val.toString();
            if (valstr.startsWith("poi_")) {
                String poiValue = valstr.substring(4);


                poiList.add(poiValue);
            } else {

                String gpsCount = valstr.substring(9);
                gpsSumCount += Integer.valueOf(gpsCount);

            }
        }

        if (!poiList.isEmpty()) {
            for (String poi : poiList) {
                outputs.write(new Text(poi), new Text(String.valueOf(gpsSumCount)), "gpsCombine");
            }
        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }

}
