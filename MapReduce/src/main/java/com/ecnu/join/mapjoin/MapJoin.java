package com.ecnu.join.mapjoin;

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
 * 实现 Map 端进行 Join 的功能
 * @author ikroal
 * @date 2019-06-12
 * @version: 1.0.0
 */
public class MapJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapJoin(), args);
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
        //将其中一个小表加载到缓存中
        job.addCacheFile(new URI(args[0]));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
