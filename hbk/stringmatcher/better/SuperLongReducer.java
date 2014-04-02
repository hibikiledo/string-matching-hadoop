package hbk.stringmatcher.better;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SuperLongReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, SuperLongValueWritable> {

    private SuperLongValueWritable valueOut = new SuperLongValueWritable();

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, SuperLongValueWritable> output, Reporter reporter)
            throws IOException {

        while( values.hasNext() ) {
            valueOut.appendValue(values.next().get());
        }

        output.collect(key, valueOut);
    }
}

