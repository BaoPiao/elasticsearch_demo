package com.guowu.hadoop.read;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

//public class EsReduce extends Reducer<Text, MapWritable, Text, Text> {
//    @Override
//    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
//        StringBuffer s = new StringBuffer();
//        for (MapWritable value : values) {
//            for (Map.Entry<Writable, Writable> writableWritableEntry : value.entrySet()) {
//                Text value1 = (Text) writableWritableEntry.getValue();
//                Text key1 = (Text) writableWritableEntry.getKey();
//                s.append(value1 + ":" + key1 + " ");
//            }
//            Text text = new Text(s.toString());
//            context.write(key, text);
//        }
//    }
//}


public class EsReduce extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        StringBuffer s = new StringBuffer();
        for (MapWritable value : values) {
            context.write(key, value);
        }
    }
}