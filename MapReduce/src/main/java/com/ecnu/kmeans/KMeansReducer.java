package com.ecnu.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 根据 Mapper 分类的结果计算出新的中心，并将新的中心写入到输出目录
 * @author ikroal
 * @date 2019-06-14
 * @version: 1.0.0
 */
public class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {

    private static final String DEFAULT_SPLITTER = ",";

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<Double>> datas = new ArrayList<>();
        //解析当前分类数据，保存到集合用于计算新的中心
        for (Text text : values) {
            String value = text.toString();
            List<Double> data = new ArrayList<>();
            for (String s : value.split(DEFAULT_SPLITTER)) {
                data.add(Double.parseDouble(s));
            }
            datas.add(data);
        }

        StringBuilder result = new StringBuilder();
        //采用列遍历，统计每列的和，然后计算平均值得出当前分类的中心
        for (int i = 1; i < datas.get(0).size(); i++) {
            double sum = 0;
            //取出第 i 列计算和
            for (List<Double> data : datas) {
                sum += data.get(i);
            }
            result.append(sum / datas.size());
            if (i != datas.get(0).size() - 1) {
                result.append(DEFAULT_SPLITTER);
            }
        }

        context.write(new Text(key.get() + DEFAULT_SPLITTER), new Text(result.toString()));
    }
}
