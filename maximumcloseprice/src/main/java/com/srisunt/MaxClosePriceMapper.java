package com.srisunt;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxClosePriceMapper extends
        Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] items = line.split(",");

        String stock = items[1];
        Float closePrice = Float.parseFloat(items[6]);

        context.write(new Text(stock), new FloatWritable(closePrice));

    }

}