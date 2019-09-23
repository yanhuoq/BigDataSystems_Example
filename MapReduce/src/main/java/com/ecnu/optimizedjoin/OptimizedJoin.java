package com.ecnu.optimizedjoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * 通过 Distributed Cache 对 Join 进行优化
 * @author ikroal
 * @date 2019-09-23
 * @version: 1.0.1
 */
public class OptimizedJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new OptimizedJoin(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <persons input> <orders input> <output>");
            return -1;
        }
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());

        job.setJarByClass(getClass());
        //广播小表
        job.addCacheFile(new URI(args[0]));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(OptimizedJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
