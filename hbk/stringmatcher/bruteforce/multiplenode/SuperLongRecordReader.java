package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

public class SuperLongRecordReader implements RecordReader<LongWritable, Text> {

    private long pos;
    private int[] requestStringSize;
    private SuperLongLineReader in;
    private int len;
    private int index = 0;

    // Constructor
    public SuperLongRecordReader(InputStream in, int[] requestStringSize, long startOffSet) {

        this.in = new SuperLongLineReader(in);
        this.requestStringSize = requestStringSize;
        pos = startOffSet;

    }

    // Read here
    // This function determine how many <key,value> pairs to be emit.
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        key.set(pos);
        len = in.readAtPos(value, pos, requestStringSize[index]);

        // Each call of this method, shift position by one
        pos++;

        // Reach EOF, reset pos and move to next index of length
        if (len == -1) {
            pos = 0;
            index++;
        }

        if(index == requestStringSize.length)
            return false;

        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}

