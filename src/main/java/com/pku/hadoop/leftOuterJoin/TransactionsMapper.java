package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;


/*
map input (longwrite, t1 p3 u1 3 330)
map output (u1_2,p3_P)
 */

class TransactionsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ' ');
        context.write(new Text(split[2]+"_2"),new Text(split[1]+"_P"));
    }
}
