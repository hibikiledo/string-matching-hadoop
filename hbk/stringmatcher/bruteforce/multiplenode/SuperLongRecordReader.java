package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

public class SuperLongRecordReader implements RecordReader<LongWritable, Text> {

    private long pos;
    private SuperLongLineReader in;
    private long startOffset;
    private int len;
    private int index = 0;
    private boolean isEnd = false;

    // Constructor
    public SuperLongRecordReader(FSDataInputStream is, long startOffSet, long blockSize) {

        in = new SuperLongLineReader(is, startOffSet, blockSize);
        this.startOffset = startOffSet;
        pos = startOffSet;

    }

    // This function determine how many <key,value> pairs to be emit.
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {

        if(isEnd) return false;

        key.set(pos);
        len = in.read(value);

        // Each call of this method, shift position by one
        pos++;

        // Reach EOF, reset pos and move to next index of length
        if (len == -1) {
            System.out.println("len == -1 recv >> @ pos:" + pos + " index:" + index );
            isEnd = true;
            return true;
        }

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
    public void close() throws IOException { in.close(); }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}

