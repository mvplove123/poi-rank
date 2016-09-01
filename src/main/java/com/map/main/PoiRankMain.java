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

/**
 * poi rank
 */
public class PoiRankMain {
    private static final String outPoiRank = "/poiRank/";

    private static final String clusterResult = "/user/go2search/taoyongbo/output/clusterResult/";

    //聚类中心
    private static String featureCenterpath = "/user/go2search/taoyongbo/output/clusterCenter/";

    //聚类中心分类路径
    private static String cityCatefeatureCenterpath = "";

    //聚类分类结果路径
    private static String cityCateClusterResultpath = "";

    public static void main(String[] args) throws Exception {

        String category="";
        String city="";

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

            String input = otherArgs[0];
            if (input.endsWith("/")) {
                int e = input.length() - 1;
                input = input.substring(0, e);
            }

            String output = otherArgs[1];
            category = otherArgs[2];
            city = otherArgs[3];
            cityCatefeatureCenterpath = featureCenterpath+city+"/"+category+"/";
            cityCateClusterResultpath = clusterResult+city+"/"+category;

            conf.set("centerpath", cityCatefeatureCenterpath);

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outPath = output + outPoiRank+city+"/"+category;

            Path out = new Path(outPath);
            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            StringBuffer sb = new StringBuffer();
            sb.append("poiRankMain:" + clusterResult + "," + cityCateClusterResultpath);


            System.out.println("input path: " + sb.toString());
            System.out.println("output path: " + out);
            long ctm = System.currentTimeMillis();
            Job job = new Job(conf, "PoiRankMain --");
            job.setJarByClass(PoiRankMain.class);


            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            MultipleInputs.addInputPath(job, new Path(cityCateClusterResultpath), TextInputFormat.class, PoiRankMapper.class);

            FileOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                System.exit(0);
            } else {
                System.err.println("PoiRank main error");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
