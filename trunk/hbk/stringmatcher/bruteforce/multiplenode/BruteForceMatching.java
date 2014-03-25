package hbk.stringmatcher.bruteforce.multiplenode;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BruteForceMatching {

    private static JobConf jobConf;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            // Debug
            System.out.println(key + ":" + value +":" + value.getLength());

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {


        JobConf conf = new JobConf(BruteForceMatching.class);
        conf.setJobName("StringMatching");

        // Todo Don't forget to change this to match reduce class & function
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(AdvancedTextWritable.class);

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

        jobConf = conf;

        JobClient.runJob(conf);


    }
}
