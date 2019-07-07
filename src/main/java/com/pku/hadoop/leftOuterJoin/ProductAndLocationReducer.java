package com.pku.hadoop.leftOuterJoin;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*          输入格式
            (u1_1,UT_L)
            (u1_2,p1_P)


            (u2_2,p3_P)
            (u2_1,UT_L)

 */
public class ProductAndLocationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        List<Text> list =  new ArrayList<>();
        while (iter.hasNext()){
            list.add(iter.next());
        }
        for (int i = 0; i < list.size(); i++) {
            Text value1 = list.get(i);
            String[] split1 = value1.toString().split("_");
            for (int j = i + 1; j < list.size(); j++) {
                Text value2 = list.get(j);
                String[] split2 = value2.toString().split("_");
                if (split1[1].equals(split2[1]))
                    continue;
                if (split1[1].equals("P"))
                    context.write(new Text(split1[0]), new Text(split2[0]));
                else
                    context.write(new Text(split2[0]), new Text(split2[0]));
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
