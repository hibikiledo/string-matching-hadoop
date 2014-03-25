package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SuperLongReducer extends MapReduceBase implements Reducer<Text, AdvancedTextWritable, Text, AdvancedTextWritable> {

    private AdvancedTextWritable outValue = new AdvancedTextWritable();

    @Override
    public void reduce(Text key, Iterator<AdvancedTextWritable> values, OutputCollector<Text, AdvancedTextWritable> output, Reporter reporter)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        while( values.hasNext() ) {
            sb.append(values.next());
            sb.append(",");
        }




        System.out.println("Reduce Output: " + key + "<>" + sb.toString());
        output.collect(key, outValue);

    }
}

