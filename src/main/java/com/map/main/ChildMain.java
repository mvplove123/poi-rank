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

/**
 * poi rank
 */
public class ChildMain {


    private static final String child = "/child";


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


            String input = otherArgs[0];
            if (input.endsWith("/")) {
                int e = input.length() - 1;
                input = input.substring(0, e);
            }
            String inputPath = input + child;


            String output = otherArgs[1];

            if (output.endsWith("/")) {
                int e = output.length() - 1;
                output = output.substring(0, e);
            }
            String outPath = output + child;

            Path out = new Path(outPath);
            FileSystem fs = out.getFileSystem(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }

            StringBuffer sb = new StringBuffer();
            sb.append("childMain:" + Constants.STRUCTURE);


            System.out.println("input path: " + sb.toString());
            System.out.println("output path: " + out);
            long ctm = System.currentTimeMillis();
            Job job = new Job(conf, "ChildMain --");
            job.setJarByClass(ChildMain.class);
            job.setReducerClass(ChildReducer.class);


            job.setInputFormatClass(TextInputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, GBKFileOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//			job.setNumReduceTasks(1);


            MultipleInputs.addInputPath(job, new Path(Constants.STRUCTURE), TextInputFormat.class, ChildMapper.class);
            MultipleInputs.addInputPath(job, new Path(Constants.MYSELF), TextInputFormat.class, ChildMapper.class);
            MultipleInputs.addInputPath(job, new Path(Constants.POI), TextInputFormat.class, ChildMapper.class);
            MultipleInputs.addInputPath(job, new Path(Constants.BUS_POI), TextInputFormat.class, ChildMapper.class);


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
