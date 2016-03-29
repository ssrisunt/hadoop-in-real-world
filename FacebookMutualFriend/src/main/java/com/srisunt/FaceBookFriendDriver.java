package com.srisunt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FaceBookFriendDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("FaceBookFriendDriver [hdfs input path] [hdfs output dir]");
            return 1;
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        Job fb = Job.getInstance(conf);
        fb.setJobName("FaceBookFriends ");
        fb.setJarByClass(getClass());

        fb.setInputFormatClass(TextInputFormat.class);
        fb.setOutputFormatClass(SequenceFileOutputFormat.class);

        //fb.setNumReduceTasks(0);
        fb.setMapperClass(FacebookFriendMapper.class);
        fb.setReducerClass(FacebookFriendReducer.class);



        fb.setMapOutputKeyClass(FriendPair.class);
        fb.setMapOutputValueClass(FriendArray.class);


        fb.setOutputKeyClass(FriendPair.class);
        fb.setOutputValueClass(FriendArray.class);


        FileInputFormat.setInputPaths(fb, inputPath);
        SequenceFileOutputFormat.setOutputPath(fb, outputPath);

        if (fb.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new FaceBookFriendDriver(), args);
        System.exit(returnCode);
    }
}
