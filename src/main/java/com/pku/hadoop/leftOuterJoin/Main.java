package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sun.jvm.hotspot.debugger.win32.coff.TestDebugInfo;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/data/chapter4/users"), TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://localhost:9000/data/chapter4/transactions"), TextInputFormat.class, TransactionsMapper.class);
        job1.setPartitionerClass(UserPartitioner.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setGroupingComparatorClass(UsersGroup.class);
        job1.setReducerClass(ProductAndLocationReducer.class);
//        job1.setReducerClass(MyRedu.class);
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/data/chapter4/output"));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(5);

        job1.waitForCompletion(true);
    }
}
