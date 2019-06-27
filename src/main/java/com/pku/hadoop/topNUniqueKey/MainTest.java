package com.pku.hadoop.topNUniqueKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/13 * @Description: com.pku.hadoop.topNUniqueKey * @version: 1.0
 */
public class MainTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("top.n","10");

        Job job = Job.getInstance(conf);
        job.setJarByClass(MainTest.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://node01:9000/data/input/cats"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://node01:9000/data/output/cats"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TopN_Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TopN_Reducer.class);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
