package hbk.stringmatcher.better;

import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BruteForceMatching {

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(BruteForceMatching.class);
        conf.setJobName("StringMatching_HBK");

        // Give hint for the key-value format of mapper class
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);

        // Give hint for value output format of the reduce class
        conf.setOutputValueClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        // Set map class and reduce class
        conf.setMapperClass(SuperLongMapper.class);
        conf.setReducerClass(SuperLongReducer.class);

        conf.setNumReduceTasks(10);

        // Set input and output format
        conf.setInputFormat(SuperLongInputFormat.class);
        conf.setOutputFormat(SuperLongOutputFormat.class);

        // Set user-defined split size in configuration file.
        conf.setInt("hbk.userdefined.split.size", Integer.parseInt(args[3]));

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Setup DistributedCache
        DistributedCache.addCacheFile(new URI(args[2]), conf);

        JobClient.runJob(conf);
    }
}