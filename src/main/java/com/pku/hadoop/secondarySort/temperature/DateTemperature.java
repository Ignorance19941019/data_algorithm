package com.pku.hadoop.secondarySort.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/11 * @Description: com.pku.hadoop.secondarySort * @version: 1.0
 */
public class DateTemperature implements WritableComparable<DateTemperature> {

    // 输入格式：2012,01,01,5
    private int year;
    private int month;
    private int temperature;

    public String getYearMonth() {
        return year + "-" + month;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(DateTemperature that) {
        int compareValue = this.getYearMonth().compareTo(that.getYearMonth());
        if (compareValue == 0)
        {
            compareValue = this.getTemperature() - that.getTemperature();
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.temperature = dataInput.readInt();
    }
}
