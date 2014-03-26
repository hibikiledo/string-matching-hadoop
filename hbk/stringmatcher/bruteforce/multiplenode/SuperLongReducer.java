package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SuperLongReducer extends MapReduceBase implements Reducer<Text, AdvancedTextWritable, Text, Text> {

    private Text outValue = new Text();
    private AdvancedTextWritable curValue;

    @Override
    public void reduce(Text key, Iterator<AdvancedTextWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        while( values.hasNext() ) {

            curValue = values.next();
            sb.append('(');
            sb.append(curValue.getOffset());
            sb.append(',');
            sb.append(curValue.getValue());
            sb.append(") ");

        }

        outValue.set(sb.toString());

        System.out.println("Reduce Output: " + key + "<>" + sb.toString());
        // Key is string, value are offset and fileName
        output.collect(key, outValue);

    }
}

