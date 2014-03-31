package hbk.stringmatcher.better;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SuperLongReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text> {

    private Text outValue = new Text();

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        StringBuilder sb = new StringBuilder();
        boolean isFirstElementPassed = false;

        while( values.hasNext() ) {
            if(!isFirstElementPassed) {
                sb.append(values.next().get());
                isFirstElementPassed = true;
            }
            else {
                sb.append(',').append(values.next().get());
            }
        }
        outValue.set(sb.toString());
        output.collect(key, outValue);
    }
}

