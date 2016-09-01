package com.map.main;

import com.map.hadoop.mr.GBKFileOutputFormat;
import com.map.hadoop.mr.KMeansMapper;
import com.map.hadoop.mr.KMeansReducer;
import com.map.kmeans.Assistance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * poi kmeans聚类
 */
public class KMeansMain {

    //聚类权重
    private static final String featureWeightPath = "/user/go2search/taoyongbo/input/featureWeight/poi-weight.txt";

    //聚类中心
    private static String featureCenterpath = "/user/go2search/taoyongbo/output/clusterCenter/";

    //聚类中心分类路径
    private static String cityCatefeatureCenterpath = "";


    //poi 特征值
    private static final String featurePoiPath = "/user/go2search/taoyongbo/output/featurePoi/";


    //poi 特征值分类路径
    private static String cityCatefeaturePoipath="";

    //聚类输出结果
    private static final String outputPath = "/clusterResult/";

    //城市聚类分类数据路径
    private static String cityCateOutputPath = "";

    //聚类结束阀值
    private static final float threshold = 0.0001f;

    private static String mainName="";

    protected static final Logger logger = LoggerFactory.getLogger(KMeansMain.class);


    public static void main(String[] args) throws Exception {

        int repeated = 0;
        String category="";
        String city="";
        Configuration conf=null;

        /*
        不断提交MapReduce作业直到相邻两次迭代聚类中心的距离小于阈值或到达设定的迭代次数
        */
        do {
            conf = new Configuration();
            conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
            conf.setBoolean("mapred.map.tasks.speculative.execution", true);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
            conf.setBoolean("mapred.compress.map.output",true);
            conf.setInt("mapred.task.timeout", 600000);

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();




            if (otherArgs.length < 2) {
                System.err.println("Usage: <int> <out> <oldcenters> <newcenters> <threshold>");
                System.exit(2);
            }

            String input = otherArgs[0];
            if (input.endsWith("/")) {
                int e = input.length() - 1;
                input = input.substring(0, e);
            }

            category = otherArgs[2];
            city = otherArgs[3];

            cityCatefeatureCenterpath = featureCenterpath+city+"/"+category+"/";

            cityCatefeaturePoipath = featurePoiPath+city+"/"+category;

            conf.set("centerpath", cityCatefeatureCenterpath);
            conf.set("weightpath", featureWeightPath);


            mainName = city+"--"+category + "--KMeansCluster";

            Job job = new Job(conf, mainName);//新建MapReduce作业

            job.setJarByClass(KMeansMain.class);//设置作业启动类

            Path in = new Path(cityCatefeaturePoipath);
            cityCateOutputPath = otherArgs[1] + outputPath +city+"/"+category;

            Path out = new Path(cityCateOutputPath);
            logger.info("cityCatefeaturePoipath:"+cityCatefeaturePoipath);
            logger.info("cityCatefeatureCenterpath:"+cityCatefeatureCenterpath);

            logger.info("cityCateOutputPath:"+cityCateOutputPath);
            FileInputFormat.addInputPath(job, in);//设置输入路径
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {//如果输出路径存在，则先删除之
                fs.delete(out, true);
            }
            FileOutputFormat.setOutputPath(job, out);//设置输出路径
            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapperClass(KMeansMapper.class);//设置Map类
            job.setReducerClass(KMeansReducer.class);//设置Reduce类

            job.setOutputKeyClass(Text.class);//设置输出键的类
            job.setOutputValueClass(Text.class);//设置输出值的类

            job.waitForCompletion(true);//启动作业

            ++repeated;
            logger.info("We have repeated " + repeated + " times.");

        }
        while (repeated < 100 && (Assistance.isFinished(cityCatefeatureCenterpath, cityCateOutputPath+"/part-r-00000",
                threshold,conf,category,repeated,city) == false));
        //根据最终得到的聚类中心对数据集进行聚类
        logger.info("start cluster");

        Cluster(args);
    }

    public static void Cluster(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
        conf.setBoolean("mapred.map.tasks.speculative.execution", true);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
        conf.setBoolean("mapred.compress.map.output",true);
        conf.setInt("mapred.task.timeout", 600000);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: <int> <out> <oldcenters> <newcenters> <threshold>");
            System.exit(2);
        }


        String input = otherArgs[0];
        if (input.endsWith("/")) {
            int e = input.length() - 1;
            input = input.substring(0, e);
        }

        conf.set("centerpath", cityCatefeatureCenterpath);
        conf.set("weightpath", featureWeightPath);

        Job job = new Job(conf, mainName);
        job.setJarByClass(KMeansMain.class);
        LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

        Path in = new Path(cityCatefeaturePoipath);
//        String outPath = otherArgs[1] + outputPath;

        Path out = new Path(cityCateOutputPath);
        FileInputFormat.addInputPath(job, in);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);

        //因为只是将样本点聚类，不需要reduce操作，故不设置Reduce类
        job.setMapperClass(KMeansMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}