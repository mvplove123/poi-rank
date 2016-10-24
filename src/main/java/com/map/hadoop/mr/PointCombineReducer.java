package com.map.hadoop.mr;

import com.map.model.CellCut;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

import java.io.IOException;

/**
 * poi Gps reducer 计算
 */
public class PointCombineReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        String structureXY="";
        String areaXY="";
        String poiXY="";
        String name="";
        for (Text val : values) {

            String valStr = val.toString();

            if(valStr.startsWith("structure_")){

                structureXY=valStr.substring(10);


            }else if (valStr.startsWith("area_")){
                areaXY=valStr.substring(5);
            }else{
                String poi=valStr.substring(4);
                name=poi.split("\t")[0];
                poiXY=poi.split("\t")[4];
            }
        }

        if (StringUtils.isNotBlank(areaXY)) {

            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{ name,areaXY});

            outputs.write(key, new Text(mapValue), "combineXY");

        }else if (StringUtils.isNotBlank(structureXY)){
            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{ name,structureXY});

            outputs.write(key, new Text(mapValue), "combineXY");
        }else{
            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{ name,poiXY});
            outputs.write(key, new Text(mapValue), "combineXY");
        }


    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }

}
