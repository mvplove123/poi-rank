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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * poi rank
 */
public class GpsCombineMain {
    private static final String gpsCombine = "/gpsCombine";

    private static final String gps = "/user/go2search/taoyongbo/output/gps/";

    private static final String point = "/user/go2search/taoyongbo/output/pointCombine/";

    protected static final Logger logger = LoggerFactory.getLogger(GpsCombineMain.class);


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
            String inputPath = point+","+ gps;


            String output = otherArgs[1];

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outPath = output + gpsCombine;
            Path out = new Path(outPath);

            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            logger.info("input path: " + inputPath);
            logger.info("output path: " + out);
            Job job = new Job(conf, "gpsCombineMain --");
            job.setJarByClass(GpsCombineMain.class);
            job.setReducerClass(GpsCombineReducer.class);
            job.setNumReduceTasks(500);
            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//            FileInputFormat.setMaxInputSplitSize(job, 1048576);
//            FileInputFormat.setMinInputSplitSize(job, 1);


            MultipleInputs.addInputPath(job, new Path(gps), TextInputFormat.class, GpsCombineMapper.class);
            MultipleInputs.addInputPath(job, new Path(point), TextInputFormat.class, GpsCombineMapper.class);
//            MultipleInputs.addInputPath(job, new Path(Constants.POI), TextInputFormat.class, GpsCombineMapper.class);
//            MultipleInputs.addInputPath(job, new Path(Constants.BUS_POI), TextInputFormat.class, GpsCombineMapper.class);



            FileOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                System.exit(0);
            } else {
                System.err.println("GpsCombine main error");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
