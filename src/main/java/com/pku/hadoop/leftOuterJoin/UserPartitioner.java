package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


// (u1_2,p3_P)
public class UserPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text user, Text item, int i) {
        return user.toString().split("_")[0].hashCode() % i;
    }
}
