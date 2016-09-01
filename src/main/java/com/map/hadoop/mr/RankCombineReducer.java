package com.map.hadoop.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * poi Gps reducer 计算
 */
public class RankCombineReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        String poiValue = "";
        String rank = "";
        for (Text val : values) {

            String valstr = val.toString();
            if (valstr.startsWith("poi_")) {
                poiValue = valstr.substring(4);
            } else {
                rank = valstr.substring(5);
            }
        }

        if (StringUtils.isNotBlank(poiValue) && StringUtils.isNotBlank(rank)) {
            outputs.write(new Text(poiValue), new Text(rank), "rankCombine");
        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }

}
