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
import com.map.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * poi rank
 */
public class CombineMain {
    private static final String combine = "/combine";

    private static final String parent = "/user/go2search/taoyongbo/output/parent"; //结构化数据
    private static final String caculate = "/user/go2search/taoyongbo/output/caculate"; //引用次数
    private static final String gpsCombine = "/user/go2search/taoyongbo/output/poiHotCount/"; //gps热点，人气值
    private static final String search = "/user/go2search/lisiwei/quanguoSearch/"; //点击数，搜索树




    public static void main(String[] args) throws Exception {
        try {
            System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
            System.setProperty("javax.xml.parsers.SAXParserFactory",
                    "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
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

            String[] inputPoiPath = input.split(",");

            if(inputPoiPath.length!= 3){
                System.err.println("inputPoiPath length is wrong,should be 3");
                System.exit(2);
            }



            String inputPath = parent + "," + caculate + "," + input;


            String output = otherArgs[1];

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outPath = output + combine;
            Path out = new Path(outPath);

            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            System.out.println("input path: " + inputPath);
            System.out.println("output path: " + out);
            Job job = new Job(conf, "combineMain --taoyongbo");
            job.setJarByClass(CombineMain.class);
            job.setReducerClass(CombineReducer.class);
            job.setNumReduceTasks(500);
            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//            FileInputFormat.setMaxInputSplitSize(job, 1048576);
//            FileInputFormat.setMinInputSplitSize(job, 1);


            MultipleInputs.addInputPath(job, new Path(caculate), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(parent), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(gpsCombine), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(search), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(inputPoiPath[0]), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(inputPoiPath[1]), TextInputFormat.class, CombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(inputPoiPath[2]), TextInputFormat.class, CombineMapper.class);

            FileOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                System.exit(0);
            } else {
                System.err.println("Stat main error");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
