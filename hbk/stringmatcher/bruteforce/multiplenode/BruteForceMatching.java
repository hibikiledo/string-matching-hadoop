package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.net.URI;

public class BruteForceMatching {

    private String stringListFileName;

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(BruteForceMatching.class);
        conf.setJobName("StringMatching_HBK");

        // Give hint for the key-value format of mapper class
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongWritable.class);

        // Give hint for value output format of the reduce class
        conf.setOutputValueClass(Text.class);
        conf.setOutputValueClass(SuperLongValueWrapper.class);

        // Set map class and reduce class
        conf.setMapperClass(SuperLongMapper.class);
        conf.setReducerClass(SuperLongReducer.class);

        conf.setInputFormat(SuperLongInputFormat.class);
        conf.setOutputFormat(SuperLongOutputFormat.class);

        conf.setInt("hbk.userdefined.splitsize", Integer.parseInt(args[3]));

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Setup DistributedCache
        DistributedCache.addCacheFile(new URI(args[2]), conf);

        JobClient.runJob(conf);
    }
}