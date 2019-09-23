package com.ecnu.kmeans;

import com.ecnu.constants.ConfigurationKey;
import com.ecnu.kmeans.utils.CentersOperation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 完成数据的分类
 * @author ikroal
 * @date 2019-06-14
 * @version: 1.0.0
 */
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<List<Double>> mCenters = null;
    private int mCenterCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取中心数据
        String centersPath = context.getConfiguration().get(ConfigurationKey.CENTERS_PATH);
        mCenters = CentersOperation.getCenters(centersPath, false);
        mCenterCount = mCenters.size();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        List<Double> data = new ArrayList<>();
        int centerIndex = 1;
        double minDistance = Double.MAX_VALUE;

        //保存数据到集合中方便后续计算
        for (String s : values) {
            data.add(Double.parseDouble(s));
        }

        //遍历处理给定的中心
        for (int i = 0; i < mCenterCount; i++) {
            double distance = 0;
            List<Double> center = mCenters.get(i);
            //计算数据与给定中心之间的距离
            for (int j = 1; j < center.size(); j++) {
                distance += Math.pow((data.get(j) - center.get(j)), 2);
            }
            distance = Math.sqrt(distance);
            //将数据分配给距离最短的中心
            if (distance < minDistance) {
                minDistance = distance;
                centerIndex = i + 1;
            }
        }

        context.write(new IntWritable(centerIndex), value);
    }
}
