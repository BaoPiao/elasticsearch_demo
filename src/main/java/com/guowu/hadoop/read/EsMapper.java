package com.guowu.hadoop.read;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EsMapper extends Mapper<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {

        context.write(key, value);
    }
}
