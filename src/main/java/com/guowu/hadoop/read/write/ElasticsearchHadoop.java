package com.guowu.hadoop.read.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class ElasticsearchHadoop {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "192.168.10.108:9200");
        conf.set("es.resource", "/artists_2");
        conf.set("es.write.operation", "upsert");
        conf.set("es.mapping.include", "id");
        ArrayList<String> strings = new ArrayList<>();
        strings.add("name");
        strings.add("id");
        conf.set("es.mapping.id", "id");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ElasticsearchHadoop.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapperClass(EsMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.10.108:9000/test"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
