package hbk.stringmatcher.better;

import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BruteForceMatching {

    private String stringListFileName;

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(BruteForceMatching.class);
        conf.setJobName("StringMatching_HBK");

        // Todo Don't forget to change this to match reduce class & function
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);

        conf.setMapperClass(SuperLongMapper.class);
        conf.setReducerClass(SuperLongReducer.class);

        conf.setInputFormat(SuperLongInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumMapTasks(4);

        // Set key-value separator to ','
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Setup DistributedCache
        DistributedCache.addCacheFile(new URI(args[2]), conf);

        JobClient.runJob(conf);
    }
}