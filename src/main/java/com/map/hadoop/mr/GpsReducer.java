package com.map.hadoop.mr;

import com.map.model.CellCut;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * poi Gps reducer 计算
 */
public class GpsReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;
    CellCut cellCut = new CellCut(50,50);/////格子范围，实际取值范围应设置在50范围内

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        int count=0;
        for (Text val : values) {
            count++;
        }

        if (count>0) {

            String currentKey = key.toString();

            //中心坐标
            String[]xy = currentKey.split("\\|");

            double currentX = Double.valueOf(xy[0]) *50+25;
            double currentY = Double.valueOf(xy[1]) *50+25;

            String outKey = String.valueOf(currentX) + "," + String.valueOf(currentY);

            outputs.write(new Text(outKey), new Text(String.valueOf(count)), "gpsCount");

        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }

}
