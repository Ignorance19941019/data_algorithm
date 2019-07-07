package com.pku.hadoop.topNRepeatKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("top.n", "5");

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);

        FileInputFormat.addInputPath(job, new Path("/data/chapter/cat1"));
        FileInputFormat.addInputPath(job, new Path("/data/chapter/cat2"));
        FileInputFormat.addInputPath(job, new Path("/data/chapter/cat3"));
        FileOutputFormat.setOutputPath(job,new Path("/data/chapter/repeatkey"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);


        job.setNumReduceTasks(1);

        job.waitForCompletion(true);


    }
}
