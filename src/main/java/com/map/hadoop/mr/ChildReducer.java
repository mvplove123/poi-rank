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
import com.map.model.CellCut;
import com.map.model.QuadTree;
import com.map.util.Convertor_LL_Mer;
import com.map.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * poi rank reducer 计算
 */
public class ChildReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;







    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        String guid = "";
        String name ="";


        for(Text val : values){

            String valstr = val.toString();

            if(valstr.startsWith("guid_")){
                guid=valstr.substring(5);
            }
            if(valstr.startsWith("name_")){
                name=valstr.substring(5);
            }
        }

        if(StringUtils.isNotEmpty(guid) && StringUtils.isNotEmpty(name)){
            outputs.write(new Text(guid),new Text(name),"child");
        }
    }



    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }





    public static void main(String[] args) {

        ChildReducer reducer = new ChildReducer();

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
