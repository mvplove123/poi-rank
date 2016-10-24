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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * poi gps 计算
 */
public class GpsMain {
    private static final String gps = "/bjgps";


    private static final String inputGpsPath = "/user/go2mapimelog/ime.full/2016_07_*/part*";

    protected static final Logger logger = LoggerFactory.getLogger(GpsMain.class);

    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
            conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
            conf.setBoolean("mapred.map.tasks.speculative.execution", true);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", true);
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setInt("mapred.task.timeout", 600000000);
            String[] otherArgs = new GenericOptionsParser(conf, args)
                    .getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("Usage: StatMain <in> <out> [type]");
                System.exit(2);
            }


            String output = otherArgs[1];

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outGpsPath = output + gps;


            Path out = new Path(outGpsPath);
            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            logger.info("input path: " + inputGpsPath);
            logger.info("output path: " + outGpsPath);

            Job job = new Job(conf, "GpsMain --");
            job.setJarByClass(GpsMain.class);
            job.setReducerClass(GpsReducer.class);


            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(500);

            job.setMapperClass(GpsMapper.class);

//            MultipleInputs.addInputPath(job, new Path(inputGpsPath), TextInputFormat.class, GpsMapper.class);
            FileInputFormat.addInputPaths(job,inputGpsPath);

            FileOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                System.exit(0);
            } else {
                System.err.println("Gps main error");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.info(e.getMessage());
        }
    }
}
