package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class MyReducer extends Reducer<DateTemperature, IntWritable, Text, Text> {
    @Override
    protected void reduce(DateTemperature key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        String yearMonth = key.getYearMonth() + ":";
        for (IntWritable value : values) {
            sb.append(value);
            sb.append(",");
        }
        context.write(new Text(yearMonth),new Text(sb.toString()));
    }
}
