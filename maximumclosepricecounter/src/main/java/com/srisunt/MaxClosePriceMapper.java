package com.srisunt;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxClosePriceMapper extends
        Mapper<LongWritable, Text, Text, FloatWritable> {

    public enum Volume {
        HIGH_VOLUME,
        LOW_VOLUME
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] items = line.split(",");

        String stock = items[1];
        Float closePrice = Float.parseFloat(items[6]);


        int volumne = Integer.parseInt(items[7]);

        if(volumne>=500000)
            context.getCounter(Volume.HIGH_VOLUME).increment(1);
        else
            context.getCounter(Volume.LOW_VOLUME).increment(1);

        context.write(new Text(stock), new FloatWritable(closePrice));

    }

}