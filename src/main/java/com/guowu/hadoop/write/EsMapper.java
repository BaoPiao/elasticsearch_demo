package com.guowu.hadoop.write;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EsMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        MapWritable hashMap = new MapWritable();

        for (String s1 : s.split(" ")) {
            if (StringUtils.isNotEmpty(s1)) {
                String[] split = s1.split(":");
                if (split.length == 2) {
                    Text text = new Text();
                    text.set(split[0]);
                    Text text1 = new Text(split[1]);
                    hashMap.put(text, text1);
                }
            }
        }
        context.write(NullWritable.get(), hashMap);
    }
}
