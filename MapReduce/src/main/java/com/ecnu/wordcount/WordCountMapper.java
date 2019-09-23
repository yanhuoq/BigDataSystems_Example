package com.ecnu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用于分割来自输入文件的数据，并进行初始统计，然后将统计数据传递给 Reducer
 * @author ikroal
 * @date 2019-06-11
 * @version: 1.0.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //分割传入的文本数据
        String[] datas = value.toString().split(" ");
        for (String data : datas) {
            context.write(new Text(data), new IntWritable(1));
        }
    }
}
