package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyRedu extends Reducer<Text, Text, NullWritable, Text> {

    private StringBuilder sb = new StringBuilder();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext())
        {
            sb.append(iter.next().toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),new Text(sb.toString()));
    }
}
