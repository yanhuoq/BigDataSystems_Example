package com.ecnu.join.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于保存 Order 的各项信息
 * @author ikroal
 * @date 2019-06-26
 * @version: 1.0.0
 */
public class Order implements Writable {

    private String mOrderNumber;

    public Order() {
    }

    public Order(String orderNumber) {
        mOrderNumber = orderNumber;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mOrderNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mOrderNumber = dataInput.readUTF();
    }

    public String getOrderNumber() {
        return mOrderNumber;
    }
}
