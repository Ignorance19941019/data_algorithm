package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;


// map input
// (Longwrite,u1 UT)


// map output
// (u1_1,UT_L)


class UsersMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ' ');
        context.write(new Text(split[0]+"_1"),new Text(split[1]+"_L"));
    }
}
