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

import com.map.model.CellCut;
import com.map.model.CalculatePoi;
import com.map.model.QuadTree;
import com.map.util.Convertor_LL_Mer;
import com.map.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.*;

/**
 * poi rank reducer 计算
 */
public class CalculateReducer extends Reducer<Text, Text, Text, Text> {

    MultipleOutputs outputs = null;
    CellCut cellCut = new CellCut(2000,6000);/////格子范围，实际取值范围应设置在500范围内
    private List<String> blackSubcate = Lists.newArrayList("地铁站出入口", "公交线路");

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {


        List<CalculatePoi> validList = new ArrayList<CalculatePoi>();
        List<CalculatePoi> poiList = new ArrayList<CalculatePoi>();
        for (Text val : values) {

            String poiValue = val.toString();
            String[] result = poiValue.split("\\t");


            String dataId = result[0];
            String name = result[1];
            String city = result[2];
            String point = result[3];
            String category = result[4];
            String subCategory = result[5];
            String alias = result[6];


            String[] xy = point.split(",");

            String x = xy[0];
            String y = xy[1];


            CalculatePoi poi = new CalculatePoi();
            poi.setDataId(dataId);
            poi.setName(name);
            poi.setCity(city);
            poi.setPoint(point);
            poi.setCategory(category);
            poi.setSubCategory(subCategory);
            poi.setAlias(alias);

            double[] ll = Convertor_LL_Mer.Mer2LL(Double.parseDouble(x), Double.parseDouble(y));

            poi.setLat(ll[0]);
            poi.setLng(ll[1]);
            poiList.add(poi);
            String currentID = cellCut.getCurrentCell(x, y);

            if (!currentID.equals(key.toString())) {
                continue;
            }

            validList.add(poi);


        }
        calcute(poiList, validList);
    }

    /**
     * 计算引用次数
     *
     * @param poiList
     * @param validList
     */
    private void calcute(List<CalculatePoi> poiList, List<CalculatePoi> validList) {

        for (CalculatePoi poi : validList) {
            StringBuilder str = new StringBuilder();
            int count = 0;

            for (CalculatePoi allPoi : poiList) {
                if (!poi.getCity().equals(allPoi.getCity()) || poi.getName().equals(allPoi.getName())) {
                    continue;
                }
                double d = Convertor_LL_Mer.DistanceLL(poi.getLat(), poi.getLng(), allPoi.getLat(), allPoi.getLng());
                if (d <= 5000.0) {

                    //current 正式名，target正式名匹配
                    boolean matches1 = Util.match(poi.getName(), allPoi.getName());
                    if (matches1 && !allPoi.getName().endsWith(poi.getName())) {
                        str.append(allPoi.getName());
                        str.append(",");
                        count++;
                        continue;
                    }

                    //current 正式名，target别名
                    if (StringUtils.isNotBlank(allPoi.getAlias())) {
                        boolean matches4 = Util.match(poi.getName(), allPoi.getAlias());
                        if (matches4 && !allPoi.getAlias().endsWith(poi.getName())) {
                            str.append(allPoi.getName());
                            str.append(",");
                            count++;
                            continue;
                        }
                    }

                    //current 别名，target正式名
                    if (StringUtils.isNotBlank(poi.getAlias()) && !blackSubcate.contains(poi.getSubCategory())) {
                        boolean matches2 = Util.match(poi.getAlias(), allPoi.getName());

                        if (matches2 && !allPoi.getName().endsWith(poi.getAlias())) {
                            str.append(allPoi.getName());
                            str.append(",");
                            count++;
                            continue;
                        }

                    }

                    //current 别名，target 别名
                    if (StringUtils.isNotBlank(poi.getAlias()) && !blackSubcate.contains(poi.getSubCategory()) &&
                            StringUtils.isNotBlank(allPoi.getAlias())) {
                        boolean matches3 = Util.match(poi.getAlias(), allPoi.getAlias());
                        if (matches3 && !allPoi.getAlias().endsWith(poi.getAlias())) {
                            str.append(allPoi.getName());
                            str.append(",");
                            count++;
                            continue;
                        }
                    }
                }
            }


            if(count > 0){
                String matchResult = count + "\t" + str.toString();
                export2File(poi, matchResult);
            }


        }
    }


    protected void cleanup(Context context) throws IOException, InterruptedException {

        super.cleanup(context);
        outputs.close();


    }


    private void export2File(CalculatePoi poi, String matchResult) {

        try {

            Joiner joiner = Joiner.on("\t");
            String logStr = joiner.join(new String[]{poi.getDataId(), poi.getCity(), poi.getPoint(), poi.getCategory(), poi
                    .getSubCategory(),
                    matchResult});
            outputs.write(new Text(poi.getName()), new Text(logStr), "realdistance");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {

        CalculateReducer reducer = new CalculateReducer();

        List<CalculatePoi> validpoi = new ArrayList<CalculatePoi>();

        CalculatePoi poi1 = new CalculatePoi();
        poi1.setName("ZUKKA PRO(双安商场店)");
        poi1.setPoint("1.29487735902306E7,4833230.27778304");
        CalculatePoi poi2 = new CalculatePoi();
        poi2.setName("");


        CalculatePoi poi3 = new CalculatePoi();
        validpoi.add(poi1);
        List<CalculatePoi> allpoi = new ArrayList<CalculatePoi>();
        allpoi.add(poi2);
        allpoi.add(poi3);

        reducer.calcute(allpoi, validpoi);


    }


}
