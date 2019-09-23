package com.ecnu.join.reducejoin;

import com.ecnu.constants.ConfigurationKey;
import com.ecnu.join.entity.Order;
import com.ecnu.join.entity.Person;
import com.ecnu.join.entity.ReduceJoinWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 解析传入的数据
 * @author ikroal
 * @date 2019-06-26
 * @version: 1.0.0
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, ReduceJoinWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        Configuration conf = context.getConfiguration();
        //获取传入数据所属文件路径
        String path = split.getPath().toString();
        //获取表名
        String personsName = conf.get(ConfigurationKey.PERSONS_NAME);
        String ordersName = conf.get(ConfigurationKey.ORDERS_NAME);
        ReduceJoinWritable writable = new ReduceJoinWritable();
        //分割数据
        String[] datas = value.toString().split("\t");
        //对 writable 对象设置相应信息和标识
        if (path.contains(personsName)) {
            Person person = new Person(datas[1], datas[2], datas[3], datas[4]);
            writable.setPerson(person);
            writable.setTag(ReduceJoinWritable.PERSON);
            context.write(new Text(datas[0]), writable);
        } else if (path.contains(ordersName)){
            Order order = new Order(datas[1]);
            writable.setOrder(order);
            writable.setTag(ReduceJoinWritable.ORDER);
            context.write(new Text(datas[2]), writable);
        }
    }
}
