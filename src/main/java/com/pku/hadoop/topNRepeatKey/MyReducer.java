package com.pku.hadoop.topNRepeatKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int N;
    private SortedMap<Integer,String> topNcats = new TreeMap<Integer,String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        N = conf.getInt("top.n",10);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
            IntWritable next = iter.next();
            sum += next.get();
        }
        topNcats.put(sum,key.toString());

    }
}
