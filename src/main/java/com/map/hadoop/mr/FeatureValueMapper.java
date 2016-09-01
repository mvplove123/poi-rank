

package com.map.hadoop.mr;

import com.map.kmeans.Assistance;
import com.map.model.FeatureRank;
import com.map.model.Rank;
import com.map.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 特征值数值化
 */

/**
 * 特征值数值化
 */
public class FeatureValueMapper extends Mapper<LongWritable, Text, Text, Text> {
    Map<String, FeatureRank> featuremap = null;
    MultipleOutputs outputs = null;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        featuremap = Assistance.getFeatureThreshold(context.getConfiguration().get("poi-feature"));
        outputs = new MultipleOutputs(context);

        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = Util.getGBKString(value);

        String[] result = str.split("\t");
        String name = result[0];
        String dataId = result[1];
        String city = result[3];
        String parentCategory = result[4];

        Map<String, String> specialCity = CategoryThreshold.getSpecialCity();


        String pinyinCity;
        if (specialCity.get(city) == null) {
            pinyinCity = Cn2Spell.converterToSpell(city);
        } else {
            pinyinCity = specialCity.get(city);
        }

        String featureValue = getRank(result, featuremap);


        Map<String, String> categoryPy = CategoryCenter.getCategoryPY();

        String cagetoryPy = categoryPy.get(parentCategory);

        String outKey = pinyinCity+"\t"+cagetoryPy;

        context.write(new Text(outKey),new Text(featureValue));


//        outputs.write(NullWritable.get(), new Text(featureValue), "featurePoi");

    }


    private static String getRank(String[] result, Map<String, FeatureRank> featuremap) {
        String name = result[0];
        String dataId = result[1];
        String point = result[2];

        String city = result[3];
        String parentCategory = result[4];
        String subCategory = result[5];
        String brand = result[6];

        String keyword = result[7];
        String comment = result[8];
        String price;
        if (result[9].contains(".")) {
            price = result[9].substring(0, result[9].indexOf("."));

        } else {
            price = result[9];
        }


        String grade = result[10];
        String matchCount = result[11];
        String area = result[12];
        String leafCount = result[13];
        String doorCount = result[14];
        String parkCount = result[15];
        String innerCount = result[16];
        String buildCount = result[17];


        String hotCount = result[18];
        String hitcount = result[19];
        String viewcount = result[20];
        String sumViewOrder = result[21];



        String categoryScore = "1";
        String tagScore = "1";
        String matchCountScore = "1";
        String gradeScore = "1";
        String commentScore = "1";
        String priceScore = "1";
        String areaScore = "1";
        String leafCountScore = "1";
        String doorCountScore = "1";
        String parkCountScore = "1";
        String innerCountScore = "1";
        String buildCountScore = "1";


        String uniqueKey = parentCategory + "-" + subCategory;

//        System.out.println(uniqueKey);
        FeatureRank featureRank = featuremap.get(uniqueKey);

        if (featureRank != null) {
            Map<String, Rank> scopeValue = featureRank.getScopeValue();

            categoryScore = featureRank.getCategoryValue();
            tagScore = tagRank(keyword, featureRank.getTagValue());

            matchCountScore = fieldRank(Integer.parseInt(matchCount), scopeValue.get("matchCount"));

            areaScore = fieldRank(Integer.parseInt(area), scopeValue.get("area"));
            leafCountScore = fieldRank(Integer.parseInt(leafCount), scopeValue.get("leafCount"));
            doorCountScore = fieldRank(Integer.parseInt(doorCount), scopeValue.get("doorCount"));
            parkCountScore = fieldRank(Integer.parseInt(parkCount), scopeValue.get("parkCount"));
            innerCountScore = fieldRank(Integer.parseInt(innerCount), scopeValue.get("innerCount"));

            buildCountScore = fieldRank(Integer.parseInt(buildCount), scopeValue.get("buildCount"));
            gradeScore = fieldRank(Integer.parseInt(grade), scopeValue.get("grade"));
            commentScore = fieldRank(Integer.parseInt(comment), scopeValue.get("comment"));
            priceScore = fieldRank(Integer.parseInt(price), scopeValue.get("price"));
        }
        Joiner joiner = Joiner.on("\t");

        //34个字段
        String featureValue = joiner.join(new String[]{
                name, dataId, city, parentCategory, subCategory, brand, categoryScore,tagScore, matchCountScore, gradeScore,
                commentScore, priceScore, areaScore, leafCountScore, doorCountScore, parkCountScore, innerCountScore, buildCountScore,
                point,keyword,matchCount,grade,comment,price,area,leafCount,doorCount,parkCount,innerCount,buildCount,
                hotCount,hitcount,viewcount,sumViewOrder
        });

        return featureValue;
    }

    /**
     * rank值
     *
     * @param field
     * @return
     */
    private static String fieldRank(int field, Rank rank) {

        //如果某特征值区间配置为空，则对应特征值为0
        if (rank == null || rank.getCategoryThreshold() == null || rank.getThresholdValue() == null) {
            return "0";
        }

        Integer[] categoryThreshold = rank.getCategoryThreshold();
        Integer[] thresholdValue = rank.getThresholdValue();

        int i = 0;
        for (; i < categoryThreshold.length; i++) {
            if (field < categoryThreshold[i]) {
                break;
            }
        }
        return String.valueOf(thresholdValue[i]);
    }





    /**
     * 关键字特征值解析映射
     *
     * @param keyword
     * @param tagValue
     * @return
     */
    private static String tagRank(String keyword, String tagValue) {

        String value = "0";

        if (StringUtils.isBlank(tagValue)) {
            return value;
        }

        if (StringUtils.isNotBlank(keyword)) {

            //关键字解析
            List<String> keywordlist = Lists.newArrayList();
            String[] a = keyword.split(",");
            for (String str : a) {

                if (StringUtils.isBlank(str)) {
                    continue;
                }

                int begin = 0;
                int end = str.length();
                if (str.contains(":")) {
                    begin = str.indexOf(":");
                }
                if (str.contains("$$") && str.endsWith("$$")) {
                    end = str.lastIndexOf("$$");
                }
                if (begin + 1 > end) {
                    continue;
                }
                String newstr = str.substring(begin + 1, end);
                String[] b = newstr.split("-");

                for (String str1 : b) {
                    keywordlist.add(str1);
                }
            }

            String[] tagValues = tagValue.split("\\|");

//            System.out.println(tagValue);
            //特征值解析
            Map<String, String> tagMap = Maps.newHashMap();
            for (String str : tagValues) {
                String[] kv = str.split(":");

                String[] tags = kv[0].split("-");
                for (String tag : tags) {
                    tagMap.put(tag, kv[1]);
                }

            }

            //map 关键字特征值映射查询
            for (String kd : keywordlist) {
                if (tagMap.get(kd) != null) {
                    value = tagMap.get(kd);
                    break;
                }
            }
        }
        return value;
    }


    public static void main(String[] args) {



        String keyword="$$NEWMANUAL_M1$$,$$TP:TP00033$$,$$TP:TP10006$$,$$LS:瑞廷酒店$$,$$FB:酒店住宿-品牌酒店-瑞廷酒店$$,$$RC:瑞廷酒店$$,$$GAODE:住宿服务-宾馆酒店-五星级宾馆$$,$$NAV:住宿-商业性住宿-星级宾馆$$,$$NAV:酒店住宿-瑞廷酒店$$,$$NAV:酒店住宿-星级宾馆-五星级$$,$$DG:艺龙酒店-豪华/五星-五星级酒店$$,$$DG:点评酒店-五星级酒店$$";
        String tagValue="";
        tagRank(keyword,tagValue);



        Map<String, FeatureRank> featuremap = Maps.newHashMap();
        String path1 = "D:\\structure\\poi-rank.txt"; //
        String path2 = "D:\\structure\\bjcombine"; //

        try {
//            BufferedReader reader = FileHandler.getReader(path1, "gb18030");
//            BufferedReader reader1 = FileHandler.getReader(path2, "gb18030");
//            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream("D:\\structure\\poi-featureValue.txt",
//                    false), "GB18030");
//            String line = "";
//            while ((line = reader.readLine()) != null) {
//                Assistance.convert(featuremap, line);
//            }
//            reader.close();
//
//            String featureValue;
//            List<String> writeRresult = Lists.newArrayList();
//
//            String line1 = "";
//            while ((line1 = reader1.readLine()) != null) {
//                String[] result = line1.split("\t");
//
//                featureValue = getRank(result, featuremap);
//                writeRresult.add(featureValue);
//            }
//
//            for (String str : writeRresult) {
//                writer.write(str + "\n");
//            }
//            writer.flush();
//            writer.close();
            System.out.println("ok");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
