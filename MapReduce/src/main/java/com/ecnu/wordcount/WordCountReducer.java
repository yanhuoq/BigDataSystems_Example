package com.ecnu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 接受来自 Mapper 的数据，对数据进行进一步的统计，并将最终统计结果写入到输出目录下
 * @author ikroal
 * @date 2019-06-11
 * @version: 1.0.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //根据 Map 端传来的统计数据进行计数
        for (IntWritable value : values) {
            sum += value.get();
        }
        //写入最终结果
        context.write(key, new IntWritable(sum));
    }
}
