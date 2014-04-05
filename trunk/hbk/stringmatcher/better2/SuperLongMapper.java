package hbk.stringmatcher.better2;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

public class SuperLongMapper extends MapReduceBase implements Mapper<LongWritable, BytesWritable, Text, LongWritable> {

    private File stringListFile;
    private Text keyOut = new Text();
    private LongWritable valueOut = new LongWritable();
    private String currentFile;

    private static HashMap<Byte, BytesArrayWrapper> map = new HashMap<Byte, BytesArrayWrapper>();

    @Override
    public void configure(JobConf job) {
        try {
            URI[] files = DistributedCache.getCacheFiles(job);
            stringListFile = new File(files[0].getPath());
            initialize(job);
        } catch (IOException e) {
            System.err.println( e );
        }
    }

    // Initialize needed data for mapping key,value pairs
    private void initialize(JobConf conf) {

        // Init hash map
        map.put((byte)97 , new BytesArrayWrapper());
        map.put((byte)98 , new BytesArrayWrapper());
        map.put((byte)99 , new BytesArrayWrapper());
        map.put((byte)100, new BytesArrayWrapper());

        // Init data in hash map
        try {

            BufferedReader reader = new BufferedReader(new InputStreamReader( new FileInputStream( stringListFile )));
            String line;
            // Load all query into hash map
            while((line=reader.readLine())!=null) {
                map.get( line.getBytes()[0] ).add(line.getBytes());
            }

        } catch (IOException e) {
            System.err.println(e);
        }

        /*
        // Debug data in hash map
        Iterator<byte[]> i = map.get((byte)97).getIterator();
        while(i.hasNext()) {
            System.out.println(new String(i.next()));
        }
        i = map.get((byte)98).getIterator();
        while(i.hasNext()) {
            System.out.println(new String(i.next()));
        }
        i = map.get((byte)99).getIterator();
        while(i.hasNext()) {
            System.out.println(new String(i.next()));
        }
        i = map.get((byte)100).getIterator();
        while(i.hasNext()) {
            System.out.println(new String(i.next()));
        }
        */
    }

    @Override
    public void map(LongWritable key, BytesWritable value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        // don't process other character except a,b,c,d
        if(value.getBytes()[0] != 97
                && value.getBytes()[0] != 98
                && value.getBytes()[0] != 99
                && value.getBytes()[0] != 100)
            return;

        // debug
        //System.out.println("Key into mapper : " + key.get() + "& Value into mapper : " + new String(value.getBytes()));

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();
        byte[] valueInBytes = value.getBytes();

        // Find match one
        Iterator<byte[]> subset = map.get( valueInBytes[0] ).getIterator();
        while(subset.hasNext()) {
            byte[] current = subset.next();
            if(isMatch( current, valueInBytes )) {
                keyOut.set(currentFile+","+new String( current )); // set key
                valueOut.set( key.get() ); // set value
                output.collect(keyOut, valueOut);
             }
        }
    }

    // Method to help if the two specified bytes are matched.
    private boolean isMatch(byte[] in, byte[] in2) {
        int size = Math.min(in.length, in2.length);
        for(int i=1; i<size; i++) {
            if(in[i] != in2[i]) {
                return false;
            }
        }
        return true;
    }
}