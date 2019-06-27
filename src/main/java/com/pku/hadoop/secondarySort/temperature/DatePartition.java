package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class DatePartition extends Partitioner<DateTemperature, IntWritable> {
    @Override
    public int getPartition(DateTemperature dateTemperature, IntWritable intWritable, int i) {
        return Math.abs(dateTemperature.getYearMonth().hashCode() % i);
    }
}
