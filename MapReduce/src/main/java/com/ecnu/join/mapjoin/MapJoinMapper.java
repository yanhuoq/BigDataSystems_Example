package com.ecnu.join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 完成 Map 端 Join
 * @author ikroal
 * @date 2019-06-12
 * @version: 1.0.0
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> mPersonTable = new HashMap<>();
    private static final int DEFAULT_COLUNM = 3;

    /**
     * 从 {@link MapJoin} 设置的缓存表中读取 Person 信息保存到集合当中
     */
    @Override
    protected void setup(Context context) throws IOException {
        URI uri = context.getCacheFiles()[0];
        FileSystem fs = getFileSystem(uri);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                fs.open(new Path(uri))));
        String content;
        //处理缓存中的小表
        while ((content = reader.readLine()) != null) {
            String[] datas = content.split("\t");
            String id = datas[0];
            StringBuilder person = new StringBuilder();
            for (int i = 1; i < DEFAULT_COLUNM; i++) {
                person.append(datas[i]).append("\t");
            }
            mPersonTable.put(id, person.toString());
        }
    }

    /**
     * 根据集合中 Person 信息和读入的 Order 信息完成 Join
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = value.toString().split("\t");
        String id = datas[datas.length - 1];
        //通过 Person 表的主键和 Order 表的外键完成 Join
        if (mPersonTable.containsKey(id)) {
            context.write(new Text(mPersonTable.get(id) + datas[1]), NullWritable.get());
        }
    }

    private FileSystem getFileSystem(URI uri) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(uri, new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }
}
