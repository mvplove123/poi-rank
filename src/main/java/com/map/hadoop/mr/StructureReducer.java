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
import com.map.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 分离父子节点及未结构化节点数据
 */
public class StructureReducer extends Reducer<Text, Text, Text, Text> {


    MultipleOutputs outputs = null;
    private List<String> importSubcate = Lists.newArrayList(
            "影剧院",
            "银行",
            "大型展览馆",
            "大型博物馆",
            "大型图书馆",
            "区县图书馆",
            "展览馆",
            "博物馆",
            "三级医院",
            "二级医院",
            "一级医院",
            "别墅",
            "高档楼盘",
            "中档楼盘",
            "楼盘",
            "居民小区",
            "高档写字楼",
            "写字楼",
            "政府驻地",
            "主要政府机关",
            "政府机关",
            "知名中学",
            "知名小学",
            "一般中学",
            "一般小学",
            "知名大学",
            "大学",
            "5A4A景点",
            "1-3A景点",
            "知名景点",
            "检测场",
            "驾校",
            "4-5星级",
            "其它星级",
            "大型商场",
            "大型超市",
            "机场",
            "航站楼",
            "大型火车站",
            "火车站",
            "长途客运站",
            "落客区",
            "电视台",
            "大型体育场馆",
            "体育场馆",
            "地名",
            "地铁站"
    );

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

        if(StringUtils.isNotBlank(poi)){
            String[] poiFileds = poi.split("\t");
            String subCategory = poiFileds[4];

            String keyword = poiFileds[11];

            //未结构化重点类,人工数据导出
            if(importSubcate.contains(subCategory) || keyword.contains("NEWMANUAL") && StringUtils.isBlank
                    (structureParentPoi)){
                outputs.write(NullWritable.get(), new Text(poi), "importPoi");
            }

            //未结构化不重点数据导出
            if (!importSubcate.contains(subCategory) && !keyword.contains("NEWMANUAL") && StringUtils.isBlank
                    (structureParentPoi) && StringUtils
                    .isBlank
                    (structureChindPoi)) {
                outputs.write(NullWritable.get(), new Text(poi), "noStructurePoi");
            }
        }
        //结构化父类poi数据导出
        if (StringUtils.isNotBlank(structureParentPoi) && StringUtils.isBlank(structureChindPoi) && StringUtils
                .isNotBlank(poi)) {
            outputs.write(NullWritable.get(), new Text(poi), "importPoi");
        }



    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


    public static void main(String[] args) {

        StructureReducer reducer = new StructureReducer();

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
