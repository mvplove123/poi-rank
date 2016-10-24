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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * parent reducer 计算
 */
public class ParentReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;
    protected static final Logger logger = LoggerFactory.getLogger(ParentReducer.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        countReduce(key,values,ctx);
//        changeDataIdReduce(key, values, ctx);
//        computeBoundXY(key,values,ctx);
    }


    private void countReduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        StringBuffer child = new StringBuffer();
        String parent = "";
        int childNum = 0;
        int doorNum = 0;
        int internalSceneryNum = 0;
        int buildNum = 0;
        int parkNum=0;
        int area=0;

        List<String> xlist = Lists.newArrayList();
        List<String> ylist = Lists.newArrayList();

        for (Text val : values) {

            String valstr = val.toString();

            if (valstr.startsWith("guid_")) {
                String chd = valstr.substring(5);

                String[] chdPro = chd.split("\t");

                String name = chdPro[0];
                String chdSubCategory = chdPro[1];
                String xy = chdPro[2];


                String[] xylist = xy.split(",");
                xlist.add(xylist[0]);
                ylist.add(xylist[1]);

                //子节点数量
                childNum++;

                //大门数量
                if (chdSubCategory.equals("大门") || name.endsWith("门") && !chdSubCategory
                        .equals("景点")) {
                    doorNum++;
                }

                //停车场数量
                if ( chdSubCategory.equals("停车场") ) {
                    parkNum++;
                }

                //内部景点数量
                if (chdSubCategory.equals("景点")) {
                    internalSceneryNum++;
                }

                //楼的数量
                if (chdSubCategory.equals("楼号") || name.endsWith("楼")) {
                    buildNum++;
                }

                child.append(name);
                child.append(",");
            }
            if (valstr.startsWith("name_")) {
                parent = valstr.substring(5);
            }
        }

        String boundXY = CellCut.getBoundXY(xlist, ylist);
        if(StringUtils.isNotBlank(boundXY)){

            String[] boundxy = boundXY.split(",");


            double x = Double.valueOf(boundxy[1]) - Double.valueOf(boundxy[0]);
            double y = Double.valueOf(boundxy[3]) - Double.valueOf(boundxy[2]);
            area = (int) (x * y);
            logger.info(boundXY+"area:"+area);

        }


        if (StringUtils.isNotEmpty(parent) && StringUtils.isNotEmpty(child.toString())) {


            Joiner joiner = Joiner.on("\t");
            String mapValue = joiner.join(new String[]{String.valueOf(area), String.valueOf(childNum), String.valueOf
                    (doorNum),String.valueOf(parkNum),
                    String.valueOf(internalSceneryNum),
                    String.valueOf(buildNum), child.toString()});

            outputs.write(new Text(parent), new Text(mapValue), "parent");
        }
    }


    private void computeBoundXY(Text key, Iterable<Text> values, Context ctx) throws IOException,
            InterruptedException {
        String parent = "";

        List<String> xlist = Lists.newArrayList();
        List<String> ylist = Lists.newArrayList();

        for (Text val : values) {

            String valstr = val.toString();

            if (valstr.startsWith("guid_")) {
                String chd = valstr.substring(5);

                String[] chdPro = chd.split("\t");
                String xy = chdPro[2];

                String[] xylist = xy.split(",");
                xlist.add(xylist[0]);
                ylist.add(xylist[1]);

            }
            if (valstr.startsWith("name_")) {
                parent = valstr.substring(5);
            }
        }

            String boundxy = CellCut.getBoundXY(xlist, ylist);

            if(StringUtils.isNotBlank(boundxy) && StringUtils.isNotBlank(parent)){
                logger.info(parent);
                String parPro[] = parent.split("\\t");
                String parentDataId = parPro[1];
                outputs.write(new Text(parentDataId), new Text(boundxy), "structureXYId");
            }


    }






    private void changeDataIdReduce(Text key, Iterable<Text> values, Context ctx) throws IOException,
            InterruptedException {
        StringBuffer child = new StringBuffer();
        String parentDataId = "";
        List<String> childDataIdlist = Lists.newArrayList();

        for (Text val : values) {

            String valstr = val.toString();

            if (valstr.startsWith("guid_")) {
                String chd = valstr.substring(5);
                String childPro[] = chd.split("\t");
                String childDataId = childPro[3];
                childDataIdlist.add(childDataId);

            }
            if (valstr.startsWith("name_")) {
                String parent = valstr.substring(5);
                String parPro[] = parent.split("\t");
                parentDataId = parPro[1];
            }
        }

        if(!childDataIdlist.isEmpty() && StringUtils.isNotEmpty(parentDataId)){

            Joiner joiner = Joiner.on(",");
            String childDataIds = joiner.join(childDataIdlist);

            outputs.write(new Text(parentDataId), new Text(childDataIds), "parentDataId");
        }

    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }





    public static void main(String[] args) {

        ParentReducer reducer = new ParentReducer();

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
