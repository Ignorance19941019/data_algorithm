package com.pku.hadoop.topNUniqueKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/13 * @Description: com.pku.hadoop.topNUniqueKey * @version: 1.0
 */
public class TopN_Reducer extends Reducer<NullWritable, Text, FloatWritable, Text> {

    private SortedMap<Float, String> finalTopNcats = new TreeMap<>();
    private int N;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("top.n"));
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] split = value.toString().split(",");
            float weight = Float.parseFloat(split[0]);
            finalTopNcats.put(weight,value.toString());
            if (finalTopNcats.size()>N){
                finalTopNcats.remove(finalTopNcats.firstKey());
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<Float, String>> set = finalTopNcats.entrySet();
        for (Map.Entry<Float, String> entry : set) {
            Float key = entry.getKey();
            String value = entry.getValue();
            context.write(new FloatWritable(key), new Text(value));
        }
    }
}
