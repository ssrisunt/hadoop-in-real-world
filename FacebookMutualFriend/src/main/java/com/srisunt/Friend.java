package com.srisunt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ssrisunt on 3/28/16.
 */
public class Friend implements WritableComparable {

    private IntWritable id;
    private Text name;
    private Text homeTown;

    public Friend() {
        this.id = new IntWritable();
        this.name = new Text();
        this.homeTown = new Text();
    }

    public Friend(IntWritable id,Text name, Text homeTown) {
        this.id = id;
        this.name = name;
        this.homeTown = homeTown;

    }


    @Override
    public int compareTo(Object o) {
        Friend f2 = (Friend)o;
        return getId().compareTo(f2.getId());
    }

    @Override
    public boolean equals(Object o) {
        Friend f2 = (Friend)o;
        return getId().equals(f2.getId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        name.write(dataOutput);
        homeTown.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        name.readFields(dataInput);
        homeTown.readFields(dataInput);
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Text getHomeTown() {
        return homeTown;
    }

    public void setHomeTown(Text homeTown) {
        this.homeTown = homeTown;
    }

    @Override
    public String toString() {
        return id+":"+name+" ";
    }
}
