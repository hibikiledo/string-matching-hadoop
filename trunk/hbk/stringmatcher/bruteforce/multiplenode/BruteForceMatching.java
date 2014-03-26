package hbk.stringmatcher.bruteforce.multiplenode;

import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BruteForceMatching {

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(BruteForceMatching.class);
        conf.setJobName("StringMatching");

        // Todo Don't forget to change this to match reduce class & function
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(SuperLongMapper.class);
        // conf.setCombinerClass(SuperLongReducer.class);
        conf.setReducerClass(SuperLongReducer.class);

        // conf.setInputFormat(TextInputFormat.class);
        conf.setInputFormat(SuperLongInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // conf.setNumMapTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Setup DistributedCache
        DistributedCache.addCacheFile(new URI("./stringlist.txt"), conf);

        JobClient.runJob(conf);
    }
}
