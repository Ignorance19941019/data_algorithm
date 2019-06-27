package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class MyMapper extends Mapper<LongWritable, Text, DateTemperature, IntWritable> {
    // 输入格式：2012,01,01,5
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DateTemperature dateTemperature = new DateTemperature();
        String line = value.toString();
        String[] split = line.split(",");
        dateTemperature.setMonth(Integer.parseInt(split[1]));
        dateTemperature.setYear(Integer.parseInt(split[0]));
        dateTemperature.setTemperature(Integer.parseInt(split[3]));
        context.write(dateTemperature,new IntWritable(Integer.parseInt(split[3])));
    }
}
