package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;

public class SuperLongMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private File stringListFile;
    private Text keyOut = new Text();
    private LongWritable valueOut = new LongWritable();
    private String currentFile;

    @Override
    public void configure(JobConf job) {
        try {
            URI[] files = DistributedCache.getCacheFiles(job);
            stringListFile = new File(files[0].getPath());
        } catch (IOException e) {
            System.err.println( e );
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        // Debug
        // System.out.println("String" + ":" + value);

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
        String line;
        while((line = reader.readLine()) != null) {
            if(line.equals(value.toString())) {
                // Key of map to be the filename and string
                keyOut.set(currentFile+','+line);

                // Value is offset
                valueOut.set( key.get() );

                output.collect(keyOut, valueOut);
                System.out.println("Map Output: " + keyOut + "<>" +  valueOut.get());
            }
        }

        reader.close();

    }
}

