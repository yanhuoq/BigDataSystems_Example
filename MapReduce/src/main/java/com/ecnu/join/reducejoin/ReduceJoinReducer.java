package com.ecnu.join.reducejoin;

import com.ecnu.join.entity.Order;
import com.ecnu.join.entity.Person;
import com.ecnu.join.entity.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 提取 Person 表和 Order 表信息，再完成 Join 操作
 * @author ikroal
 * @date 2019-06-11
 * @version: 1.0.0
 */
public class ReduceJoinReducer extends Reducer<Text, ReduceJoinWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context) throws IOException, InterruptedException {
        List<Person> persons = new ArrayList<>();
        List<Order> orders = new ArrayList<>();
        //收集 persons 表和 orders 表信息
        for (ReduceJoinWritable value : values) {
            String tag = value.getTag();
            if (tag.equals(ReduceJoinWritable.PERSON)) {
                persons.add(value.getPerson());
            } else if (tag.equals(ReduceJoinWritable.ORDER)) {
                orders.add(value.getOrder());
            }
        }

        //构造结果集
        for (Person person : persons) {
            for (Order order : orders) {
                String result = person.getLastName() + "\t" +
                        person.getFirstName() + "\t" +
                        order.getOrderNumber();
                context.write(new Text(result), NullWritable.get());
            }
        }
    }
}
