package com.pku.hadoop.leftOuterJoin;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UsersGroup extends WritableComparator {

    protected UsersGroup() {
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text u1 = (Text) a;
        Text u2 = (Text) b;
        return u1.toString().split("_")[0].compareTo(u2.toString().split("_")[0]);
    }
}
