package com.ecnu.join.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于保存 Person 的各项信息
 * @author ikroal
 * @date 2019-06-26
 * @version: 1.0.0
 */
public class Person implements Writable {

    private String mLastName;
    private String mFirstName;
    private String mAddress;
    private String mCity;

    public Person() {
    }

    public Person(String lastName, String firstName, String address, String city) {
        mLastName = lastName;
        mFirstName = firstName;
        mAddress = address;
        mCity = city;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mLastName);
        dataOutput.writeUTF(mFirstName);
        dataOutput.writeUTF(mAddress);
        dataOutput.writeUTF(mCity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mLastName = dataInput.readUTF();
        mFirstName = dataInput.readUTF();
        mAddress = dataInput.readUTF();
        mCity = dataInput.readUTF();
    }

    public String getLastName() {
        return mLastName;
    }

    public String getFirstName() {
        return mFirstName;
    }

    public String getAddress() {
        return mAddress;
    }

    public String getCity() {
        return mCity;
    }
}
