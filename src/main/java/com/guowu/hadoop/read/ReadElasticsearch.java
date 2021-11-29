package com.guowu.hadoop.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import java.io.IOException;

public class ReadElasticsearch {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", "192.168.10.108:9200");
        conf.set("es.resource", "artists_2");
        conf.set("es.query", "{\n" +
                "    \"query\":{\n" +
                "        \"match_all\":{}\n" +
                "    }\n" +
                "}");
        org.apache.hadoop.mapreduce.Job job = Job.getInstance(conf);
        job.setInputFormatClass(EsInputFormat.class);

        job.setReducerClass(EsReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);


//        MapFileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\outputES"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\outputES"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
