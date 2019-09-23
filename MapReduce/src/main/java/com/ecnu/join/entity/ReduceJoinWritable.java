package com.ecnu.join.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 存储 Person 或 Order 的信息，方便传递信息到 Reducer
 * @author ikroal
 * @date 2019-06-26
 * @version: 1.0.0
 */
public class ReduceJoinWritable implements Writable {

    private Person mPerson;
    private Order mOrder;
    /**
     * 标识当前对象包含的信息是 {@link Order} 还是 {@link Person}
     */
    private String mTag;

    public static final String PERSON = "1";
    public static final String ORDER = "2";

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mTag);
        if (mTag.equals(PERSON)) {
            mPerson.write(dataOutput);
        } else {
            mOrder.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mTag = dataInput.readUTF();
        if (mTag.equals(PERSON)) {
            mPerson = new Person();
            mPerson.readFields(dataInput);
        } else {
            mOrder = new Order();
            mOrder.readFields(dataInput);
        }
    }

    public Person getPerson() {
        return mPerson;
    }

    public void setPerson(Person person) {
        mPerson = person;
    }

    public Order getOrder() {
        return mOrder;
    }

    public void setOrder(Order order) {
        mOrder = order;
    }

    public String getTag() {
        return mTag;
    }

    public void setTag(String tag) {
        mTag = tag;
    }
}
