package com.ecnu.kmeans;

import com.ecnu.constants.ConfigurationKey;
import com.ecnu.kmeans.utils.CentersOperation;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 本程序将会将数据划分为 3 个聚类，基本程序执行过程是：
 * 1. 计算数据与 3 个中心之间的距离，归并同一类的数据
 * 2. 根据归并结果计算出新的中心
 * 3. 比较新旧中心数据，如果不相同重复 1～2，如果相同则结束计算，输出结果。
 * @author ikroal
 * @date 2019-06-14
 * @version: 1.0.0
 */
public class KMeans extends Configured implements Tool {

    /**
     * 用于标识迭代计算的状态，初始值表明迭代未结束
     */
    private boolean mIsEnd = false;

    public KMeans() {
    }

    public KMeans(boolean isEnd) {
        mIsEnd = isEnd;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        //进行迭代计算直到中心不在变化
        while (true) {
            exitCode = ToolRunner.run(new KMeans(), args);
            if (exitCode == -1) {
                break;
            }

            //如果新旧中心数据相同，则意味着迭代计算结束，应当输出计算结果
            if (CentersOperation.compareCenters(args[1], args[2])) {
                ToolRunner.run(new KMeans(true), args);
                break;
            }
        }
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <data_path> <center_path> <output_path>");
            return -1;
        }
        //设置中心数据路径，方便后续进行读取
        getConf().set(ConfigurationKey.CENTERS_PATH, args[1]);
        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //迭代计算结束时需要输出的是聚类结果，不需要再通过 Reducer 重新计算中心，所以不再需要设置 Reducer
        if (!mIsEnd) {
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
