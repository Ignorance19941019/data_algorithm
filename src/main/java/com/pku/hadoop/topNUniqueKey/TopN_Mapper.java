package com.pku.hadoop.topNUniqueKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/13 * @Description: com.pku.hadoop.topNUniqueKey * @version: 1.0
 */
public class TopN_Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private SortedMap<Float, String> topNCats = new TreeMap<>();
    private int N;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("top.n"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        float weight = Float.valueOf(split[0]);
        topNCats.put(weight, value.toString());
        if (topNCats.size() > N) {
            topNCats.remove(topNCats.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<Float, String>> set = topNCats.entrySet();
        for (Map.Entry<Float, String> entry : set) {
            String value = entry.getValue();
            context.write(NullWritable.get(), new Text(value));
        }
    }
}
