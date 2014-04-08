package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

public class SuperLongMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private File stringListFile;
    private Text keyOut = new Text();
    private LongWritable valueOut = new LongWritable();
    private String currentFile;
    private ArrayList<String> query = new ArrayList<String>();

    @Override
    public void configure(JobConf job) {
        try {
            URI[] files = DistributedCache.getCacheFiles(job);
            stringListFile = new File(files[0].getPath());
            initQuery();
        } catch (IOException e) {
            System.err.println( e );
        }
    }

    private void initQuery() throws IOException{
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
        String line;
        while((line=reader.readLine())!=null) {
            query.add(line);
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();
        String v = value.toString();

        for(String q : query) {

            if( q.equals( v.substring(0, Math.min(v.length(), q.length()))) ) {
                // Key of map to be the filename and string
                keyOut.set(currentFile+','+q);

                // Value is offset
                valueOut.set( key.get() );

                output.collect(keyOut, valueOut);
                //System.out.println("Map Output: " + keyOut + "<>" +  valueOut.get());
            }
        }
    }
}