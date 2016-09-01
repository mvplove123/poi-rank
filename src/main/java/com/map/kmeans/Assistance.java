package com.map.kmeans;

import com.map.main.ClusterCenterMain;
import com.map.model.CalculatePoi;
import com.map.model.FeatureRank;
import com.map.model.Rank;
import com.map.util.CategoryThreshold;
import com.map.util.FileHandler;
import com.map.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.guava.common.base.Function;
import org.apache.hadoop.thirdparty.guava.common.collect.*;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class Assistance {

    protected static final Logger logger = LoggerFactory.getLogger(Assistance.class);

    /**
     * 读取加载聚类中心点信息：聚类分类、聚类中心label、聚类中心点
     *
     * @param inputpath
     * @return
     */
    public static Multimap<String, Map<String, List<Double>>> getCenters(String inputpath) {
        //分类聚类中心存储结构
        Multimap<String, Map<String, List<Double>>> cateCenters = ArrayListMultimap.create();




        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path inPath = new Path(inputpath);

            List<Path> centerPath = Util.getFilesUnderFolder(hdfs,inPath,null);


            Path in = centerPath.get(0);


            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {
                Map<String, List<Double>> cateMap = Maps.newHashMap();
                String record = Util.getGBKString(line);
                String[] fields = record.split("\\t");

                String city = fields[2];
                String label = fields[3];
                String category = fields[4];

                List<Double> tmplist = new ArrayList<Double>();
                for (int i = 5; i < fields.length; i++) {
                    if (StringUtils.isNotBlank(fields[i])) {
                        tmplist.add(Double.parseDouble(fields[i]));
                    }
                }
                cateMap.put(label, tmplist);
                String key = city + "-" + category;
                cateCenters.put(key, cateMap);
            }
            fsIn.close();
        } catch (IOException e) {
            logger.info("读取聚类中心异常：" + e.getMessage());
            e.printStackTrace();
        }

        return cateCenters;
    }


    /**
     * 获取中心点rank
     * @param inputpath
     * @return
     */
    public static Map<String, String> getCentersRankMap(String inputpath) {
        //分类聚类中心存储结构

        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path in = new Path(inputpath);
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();

            int sum = 0;
            List<CalculatePoi> centerList = Lists.newArrayList();

            Map<String, Double> oraMap = Maps.newHashMap();
            while (lineIn.readLine(line) > 0) {
                String record = Util.getGBKString(line);
                String[] fields = record.split("\\t");

                //数量
                String count = fields[0];
                //平均值
                String mean = fields[1];
                sum += Integer.valueOf(count);
                //城市
                String city = fields[2];
                //聚类label
                String label = fields[3];
                //拼音分类
                String category = fields[4];
                CalculatePoi poi = new CalculatePoi();
                poi.setCity(city);
                poi.setCategory(category);
                poi.setLabel(Integer.valueOf(label));
                poi.setMatchCount(Integer.valueOf(count));
                poi.setMean(Double.valueOf(mean));
//                centerList.add(poi);

                String key = city + "-" + label + "-" + category;
                oraMap.put(key,Double.valueOf(mean));
            }
            fsIn.close();


            Map<String, String> rankMap = RankKMeans.getRankMap(oraMap);

//            //按照权重中心值降序排序
//            Ordering<CalculatePoi> ordering = Ordering.natural().reverse().onResultOf(new Function<CalculatePoi,
//                    Double>() {
//                public Double apply(CalculatePoi center) {
//                    return center.getMean();
//                }
//            });
//            List<CalculatePoi> orderCenterList = ordering.immutableSortedCopy(centerList);


            return rankMap;


        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 计算权重中心占比
     * @param sum
     * @param orderCenterList
     * @return
     */
    public static Map<String, String> getRankMap(int sum, List<CalculatePoi> orderCenterList) {

        Map<String, String> rankMap = Maps.newHashMap();

        double sumPresent = 0;
        for (CalculatePoi centerLabel : orderCenterList) {

            logger.info("mean:" + centerLabel.getMean());

            //分类占比
            double present = Double.valueOf(centerLabel.getMatchCount()) / sum;

            //累计占比
            sumPresent += present;

            //获取分类rank 映射配置
            Map<String, Rank> cateRank = CategoryThreshold.getCategoryRank();

            Rank scoreRank = cateRank.get(centerLabel.getCategory());

            logger.info("sumPresent:" + sumPresent);

            String rank = fieldRank(sumPresent, scoreRank);


            //rank值和累计占比
            String value = rank + "\t" + sumPresent;

            String key = centerLabel.getCity() + "-" + centerLabel.getLabel() + "-" + centerLabel.getCategory();

            rankMap.put(key, value);


        }

        return rankMap;


    }


    /**
     * rank值
     *
     * @param field
     * @return
     */
    private static String fieldRank(double field, Rank rank) {

        //如果某特征值区间配置为空，则对应特征值为0
        if (rank == null || rank.getRankThreshold() == null || rank.getRankThresholdValue() == null) {
            return "0";
        }

        Double[] categoryThreshold = rank.getRankThreshold();
        Integer[] thresholdValue = rank.getRankThresholdValue();


        int i = 0;
        for (; i < categoryThreshold.length; i++) {
            if (field < categoryThreshold[i]) {
                break;
            }
        }
        return String.valueOf(thresholdValue[i]);
    }


    /**
     * 加载不同分类权值配置文件
     *
     * @param inputpath
     * @return
     */
    public static Map<String, List<Double>> getWeitht(String inputpath) {

        //分类权重以KV形式存储
        Map<String, List<Double>> result = Maps.newHashMap();
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path in = new Path(inputpath);
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {

                String record = Util.getGBKString(line);

                String[] weights = record.split("\t");

                String catekey = weights[0];
                List<Double> tmplist = new ArrayList<Double>();

                for (int i = 2; i < weights.length; i++) {
                    if (StringUtils.isNotBlank(weights[i])) {
                        tmplist.add(Double.parseDouble(weights[i]));
                    }
                }
                result.put(catekey, tmplist);

            }
            fsIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return result;
    }


    /**
     * 加载解析特征值数值化配置文件
     *
     * @param inputpath
     * @return
     */
    public static Map<String, FeatureRank> getFeatureThreshold(String inputpath) {

        Map<String, FeatureRank> featuremap = Maps.newHashMap();

        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path in = new Path(inputpath);
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {
                String record = Util.getGBKString(line);

                convert(featuremap, record);
            }
            fsIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return featuremap;
    }


    /**
     * 转换设值
     *
     * @param featuremap
     * @param record
     */
    public static void convert(Map<String, FeatureRank> featuremap, String record) {


        FeatureRank featureRank = new FeatureRank();//特征值对象
        Map<String, Rank> scopeValue = Maps.newHashMap();//各属性特征值取值范围
        String[] fields = record.split("\t");
        String parentCategory = fields[0];
        String subCategory = fields[1];
        String category = fields[2];
        String tag = fields[3];
        String matchCount = fields[4];
        String grade = fields[5];
        String comment = fields[6];
        String price = fields[7];
        String area = fields[8];
        String leafCount = fields[9];
        String doorCount = fields[10];
        String parkCount = fields[11];
        String innerCount = fields[12];
        String buildCount = fields[13];

        String uniqueKey = parentCategory + "-" + subCategory;


        scopeValue(scopeValue, "matchCount", matchCount);
        scopeValue(scopeValue, "grade", grade);
        scopeValue(scopeValue, "comment", comment);
        scopeValue(scopeValue, "price", price);
        scopeValue(scopeValue, "area", area);
        scopeValue(scopeValue, "leafCount", leafCount);
        scopeValue(scopeValue, "doorCount", doorCount);
        scopeValue(scopeValue, "parkCount", parkCount);
        scopeValue(scopeValue, "innerCount", innerCount);
        scopeValue(scopeValue, "buildCount", buildCount);

        featureRank.setCategoryValue(category);
        featureRank.setTagValue(tag);
        featureRank.setScopeValue(scopeValue);

        featuremap.put(uniqueKey, featureRank);
    }


    /**
     * str 数组转Integer数组
     *
     * @param str
     * @return
     */
    public static Integer[] strToIntegers(String str) {

        if (StringUtils.isBlank(str)) {
            return null;
        }

        String[] values = str.split(",");
        Integer[] n = new Integer[values.length];
        for (int i = 0; i < values.length; i++) {
            if (StringUtils.isNotBlank(values[i])) {
                n[i] = Integer.parseInt(values[i]);
            }
        }

        return n;

    }


    /**
     * 范围特征值初始化
     *
     * @param scopeValue
     * @param fieldName
     * @param fieldValue
     */
    public static void scopeValue(Map<String, Rank> scopeValue, String fieldName, String fieldValue) {
        if (StringUtils.isNotBlank(fieldValue)) {

            String[] kv = fieldValue.split("\\|");
            Integer[] fieldThreshold = strToIntegers(kv[0]);
            Integer[] thresholdValue = strToIntegers(kv[1]);
            Rank rank = new Rank(fieldThreshold, thresholdValue);
            scopeValue.put(fieldName, rank);
        } else {
            scopeValue.put(fieldName, null);
        }
    }


    //删除上一次MapReduce作业的结果
    public static void deleteLastResult(String path) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path path1 = new Path(path);
            hdfs.delete(path1, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //计算相邻两次迭代结果的聚类中心的距离，判断是否满足终止条件
    public static boolean isFinished(String oldpath, String newpath, float threshold, Configuration conf, String
            category, int repeated, String city)
            throws IOException {

        logger.info("new path" + newpath);

        Multimap<String, Map<String, List<Double>>> oldcenters = Assistance.getCenters(oldpath);
        Multimap<String, Map<String, List<Double>>> newcenters = Assistance.getCenters(newpath);

        for (String cateKey : oldcenters.keySet()) {


            Collection<Map<String, List<Double>>> oldlabelmap = oldcenters.get(cateKey);
            Collection<Map<String, List<Double>>> newlabelmap = newcenters.get(cateKey);

            if (newlabelmap.isEmpty()) {
                System.out.println("new map is empty");
            }

            double currentDist = 0;

            for (Iterator oit = oldlabelmap.iterator(); oit.hasNext(); ) {

                Map<String, List<Double>> oldlabelFeature = (Map<String, List<Double>>) oit.next();


                for (Iterator nit = newlabelmap.iterator(); nit.hasNext(); ) {
                    Map<String, List<Double>> newlabelFeature = (Map<String, List<Double>>) nit.next();
                    //计算新旧同类 所有label 差值
                    for (String lableKey : oldlabelFeature.keySet()) {

                        List<Double> oldfeatureValue = oldlabelFeature.get(lableKey);
                        if (newlabelFeature.get(lableKey) == null) {
                            break;
                        }
                        List<Double> newfeatureValue = newlabelFeature.get(lableKey);

                        //计算新旧同类 相同label 差值
                        for (int i = 0; i < oldfeatureValue.size(); i++) {
                            double tmp = Math.abs(oldfeatureValue.get(i) - newfeatureValue.get(i));
                            currentDist += Math.pow(tmp, 2);
                        }

                    }
                }

            }

            double distance = Math.sqrt(currentDist);

            logger.info("currentCity" + city + "cateKey = " + cateKey + "Distance = " + distance + " Threshold =" +
                    " " +
                    threshold);

            //任何一类不符合阀值，则更新聚类中心

            if (distance > threshold) {
                break;
            }
            return true;
        }

        /*
        如果不满足终止条件，则用本次迭代的聚类中心更新聚类中心
        */
        Assistance.deleteLastResult(oldpath);
        FileSystem hdfs = FileSystem.get(conf);

        String localPath = "/search/odin/taoyongbo/rank/cateCenter/" + city + "/" + category + "/clusterCenter-r-00000";
        String localtmpPath = "/search/odin/taoyongbo/rank/tmpCateCenter/" + city + "/" + category +
                "/clusterCenter-r-00000" + repeated;
        hdfs.copyToLocalFile(new Path(newpath), new Path(localtmpPath));

        hdfs.copyToLocalFile(new Path(newpath), new Path(localPath));
        hdfs.delete(new Path(oldpath), true);
        hdfs.copyFromLocalFile(new Path(localPath), new Path(oldpath));
        return false;
    }


    public static void main(String[] args) {

        Map<String, FeatureRank> featuremap = Maps.newHashMap();
        String path1 = "D:\\structure\\poi-rank.txt"; //

        try {
            BufferedReader reader = FileHandler.getReader(path1, "gb18030");
            String line = "";
            while ((line = reader.readLine()) != null) {
                convert(featuremap, line);
            }
            reader.close();

            System.out.println("ok");


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}