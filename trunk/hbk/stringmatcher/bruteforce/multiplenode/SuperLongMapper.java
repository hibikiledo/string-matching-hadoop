package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;

public class SuperLongMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, AdvancedTextWritable> {

    private File stringListFile;
    private Text keyOut = new Text();
    private AdvancedTextWritable valueOut = new AdvancedTextWritable();
    private String currentFile;

    @Override
    public void configure(JobConf job) {
        stringListFile = new File("./stringlist.txt");
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, AdvancedTextWritable> output, Reporter reporter) throws IOException {

        // Debug
        // System.out.println(key + ":" + value +":" + value.getLength());

        // Get name
        currentFile = ((FileSplit)reporter.getInputSplit()).getPath().getName();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(stringListFile)));
        String line;
        while((line = reader.readLine()) != null) {
            if(line.equals(value.toString())) {
                // Key of map is filename
                keyOut.set(currentFile);

                // Values contain offset, result, string
                valueOut.setOffset( key.get() );
                valueOut.setValue( line );

                output.collect(keyOut, valueOut);
                System.out.println("Map Output: " + keyOut + "<>" +  valueOut);
            }
        }

    }
}

