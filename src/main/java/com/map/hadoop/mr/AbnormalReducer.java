/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: StatReducer.java
 * @Package: com.sogou.map.hadoop.mr
 * @author: huajin.shen
 * @date: 2015年7月16日 上午11:46:09
 * @version: v1.0
 */
package com.map.hadoop.mr;

import com.map.model.CalculatePoi;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 分离父子节点及未结构化节点数据
 */
public class AbnormalReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        String structureParentPoi = "";
        String structureChindPoi = "";
        String poi = "";

        for (Text val : values) {

            String valstr = val.toString();

            if (valstr.startsWith("parentId_")) {
                structureParentPoi = valstr;
            }
            if (valstr.startsWith("childId_")) {
                structureChindPoi = valstr;
            }
            if (valstr.startsWith("poi_")) {
                poi = valstr.substring(4);
            }

        }

        if (StringUtils.isNotBlank(structureParentPoi) && StringUtils.isBlank(structureChindPoi)) {
            outputs.write(NullWritable.get(), new Text(poi), "structureParentPoi");
        }

        if (StringUtils.isBlank(structureParentPoi) && StringUtils.isBlank(structureChindPoi)) {
            outputs.write(NullWritable.get(), new Text(poi), "noStructurePoi");
        }

    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


    public static void main(String[] args) {

        AbnormalReducer reducer = new AbnormalReducer();

        List<CalculatePoi> validpoi = new ArrayList<CalculatePoi>();

        CalculatePoi poi1 = new CalculatePoi();
        poi1.setName("北京众星保洁公司(丰台区店)");
        poi1.setPoint("1.29551444340033E7,4814442.54409315");
        CalculatePoi poi2 = new CalculatePoi();
        poi2.setName("");


        CalculatePoi poi3 = new CalculatePoi();
        validpoi.add(poi1);
        List<CalculatePoi> allpoi = new ArrayList<CalculatePoi>();
        allpoi.add(poi2);
        allpoi.add(poi3);


    }


}
