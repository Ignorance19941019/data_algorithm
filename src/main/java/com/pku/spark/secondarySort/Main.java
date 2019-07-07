package com.pku.spark.secondarySort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Auther: xuemengdong
 * @Date: 2019/6/12 * @Description: com.pku.spark.secondarySort * @version: 1.0
 */
public class Main {

    // 输入格式：2012,01,01,5
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("secondarySort");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> javaRDD = sc.textFile("hdfs://node01:9000/data/input/tq");

        JavaPairRDD<String, Iterable<String>> group = javaRDD.groupBy(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                String[] split = line.split(",");
                return split[0] + "-" + split[1];
            }
        });

        group.map(new Function<Tuple2<String, Iterable<String>>, String>() {
            @Override
            public String call(Tuple2<String, Iterable<String>> elem) throws Exception {
                Iterator<String> iter = elem._2().iterator();
                ArrayList<Integer> list = new ArrayList<>();
                while (iter.hasNext()) {
                    String s = iter.next();
                    String[] split = s.split(",");
                    list.add(Integer.parseInt(split[3]));
                }
                Collections.sort(list);
                StringBuilder sb = new StringBuilder();
                for (Integer integer : list) {
                    sb.append(integer);
                    sb.append(",");
                }
                return elem._1 + ":" + sb.toString();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }
}
