package com.ecnu.join;

import com.ecnu.constants.ConfigurationKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 实现 Join 功能
 * @author ikroal
 * @date 2019-09-23
 * @version: 1.0.1
 */
public class Join extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Join(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: <input> <output> <persons table name> <orders table name>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);


        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        Configuration conf = job.getConfiguration();
        //设置 Person 表的文件名和 Order 表的文件名方便后续分类处理
        conf.set(ConfigurationKey.PERSONS_NAME, args[2]);
        conf.set(ConfigurationKey.ORDERS_NAME, args[3]);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
