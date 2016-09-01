/**
 * Copyright (C), 2010-2015, Beijing Sogo Co., Ltd.
 *
 * @Title: StatMain.java
 * @Package: com.sogou.map.hadoop.main
 * @author: huajin.shen
 * @date: 2015年7月16日 下午2:09:25
 * @version: v1.0
 */
package com.map.main;

import com.map.hadoop.mr.*;
import com.map.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 聚类中心初始计算
 */
public class ClusterCenterMain {
    private static final String clusterCenter = "/clusterCenter";

    //聚类权重
    private static final String featureWeightPath = "/user/go2search/taoyongbo/input/featureWeight/poi-weight.txt";

    //poi 特征值
    private static final String featurePoiPath = "/user/go2search/taoyongbo/output/featurePoi";

    protected static final Logger logger = LoggerFactory.getLogger(ClusterCenterMain.class);



    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
            conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
            conf.setBoolean("mapred.map.tasks.speculative.execution", true);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setInt("mapred.task.timeout", 600000);
            String[] otherArgs = new GenericOptionsParser(conf, args)
                    .getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("Usage: StatMain <in> <out> [type]");
                System.exit(2);
            }


            conf.set("weightpath", featureWeightPath);

            String input = otherArgs[0];
            if (input.endsWith("/")) {
                int e = input.length() - 1;
                input = input.substring(0, e);
            }
            String output = otherArgs[1];

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outCaculatePath = output + clusterCenter;


            Path out = new Path(outCaculatePath);
            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            StringBuffer sb = new StringBuffer();
            sb.append("caculatePath:" + outCaculatePath);


            logger.info("input path: " + featurePoiPath);
            logger.info("output path: " + sb.toString());

            System.out.println("input path: " + featurePoiPath);
            System.out.println("output path: " + sb.toString());
            long ctm = System.currentTimeMillis();
            Job job = new Job(conf, "ClusterCenterReducer --");
            job.setJarByClass(ClusterCenterMain.class);
            job.setReducerClass(ClusterCenterReducer.class);


            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);


            Path basePath = new Path(featurePoiPath);
            List<Path> allCityCatePaths = Util.getFilesUnderFolder(fs, basePath, null);

            for (Path path : allCityCatePaths) {
                MultipleInputs.addInputPath(job, path, TextInputFormat.class, ClusterCenterMapper.class);
            }
            FileOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                System.exit(0);
            } else {
                System.err.println("ClusterCenterReducer main error");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
