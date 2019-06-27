package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class GroupDateComparator extends WritableComparator {

    public GroupDateComparator(){
        super(DateTemperature.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperature w1 = (DateTemperature) a;
        DateTemperature w2 = (DateTemperature) b;
        return w1.getYearMonth().compareTo(w2.getYearMonth());
    }
}
