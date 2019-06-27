package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class MainTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();



        Job job = Job.getInstance(conf);
        FileInputFormat.addInputPath(job, new Path("hdfs://node01:9000/data/input/tq"));
        FileSystem fs = FileSystem.get(conf);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://node01:9000/data/output"));
        job.setJarByClass(MainTest.class);

        job.setInputFormatClass(TextInputFormat.class);
        Object


        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(DateTemperature.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(DatePartition.class);

        job.setGroupingComparatorClass(GroupDateComparator.class);
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);


    }


}
