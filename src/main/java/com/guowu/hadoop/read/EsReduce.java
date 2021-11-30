package com.guowu.hadoop.read;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class EsReduce extends Reducer<Text, MapWritable, Text, Text> {
    private final static String METADATA = "_metadata";

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        StringBuffer s = new StringBuffer();
        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> writableWritableEntry : value.entrySet()) {
                Text key1 = (Text) writableWritableEntry.getKey();
                if (METADATA.equalsIgnoreCase(writableWritableEntry.getKey().toString())) {
                    LinkedMapWritable linkedMapWritable = (LinkedMapWritable) writableWritableEntry.getValue();
                    s.append(key1+ " ");
                    for (Map.Entry<Writable, Writable> writableEntry : linkedMapWritable.entrySet()) {
                        Text key2 = (Text) writableEntry.getKey();
                        Text value1 = (Text) writableEntry.getValue();
                        s.append(key2 + ":" + value1 + " ");
                    }
                } else {
                    Text value1 = (Text) writableWritableEntry.getValue();
                    s.append(key1 + ":" + value1 + " ");
                }
            }
            Text text = new Text(s.toString());
            context.write(key, text);
        }
    }
}