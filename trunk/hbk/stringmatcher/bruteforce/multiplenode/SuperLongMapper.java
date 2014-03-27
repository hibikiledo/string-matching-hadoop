package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;

public class SuperLongMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, AdvancedTextWritable> {

    private File stringListFile;
    private Text keyOut = new Text();
    private AdvancedTextWritable valueOut = new AdvancedTextWritable();
    private String currentFile;

    @Override
    public void configure(JobConf job) {
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(job);
            stringListFile = new File(files[0].getName());
        } catch (IOException e) {
            System.err.println( e );
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, AdvancedTextWritable> output, Reporter reporter) throws IOException {

        // Debug
        // System.out.println("String" + ":" + value);

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
        String line;
        while((line = reader.readLine()) != null) {
            if(line.equals(value.toString())) {
                // Key of map is the pattern the matched
                keyOut.set(line);

                // Values contain offset, result, string
                valueOut.setOffset( key.get() );
                valueOut.setValue( currentFile );

                output.collect(keyOut, valueOut);
                System.out.println("Map Output: " + keyOut + "<>" +  valueOut.getOffset() + ":" + valueOut.getValue());
            }
        }

        reader.close();

    }
}

