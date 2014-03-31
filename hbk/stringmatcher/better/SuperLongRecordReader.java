package hbk.stringmatcher.better;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;

public class SuperLongRecordReader implements RecordReader<LongWritable, BytesWritable> {

    private long pos;
    private long endOffset;
    private SuperLongLineReader in;
    private int len;

    // Constructor
    public SuperLongRecordReader(InputStream is, long startOffSet, long blockSize, int maxPatLength) {
        in = new SuperLongLineReader(is, startOffSet, blockSize, maxPatLength);
        pos = startOffSet;
        endOffset = startOffSet + blockSize + (maxPatLength-1);
    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {
        key.set(pos);
        len = in.read(value);
        pos++;  // Each call of this method, shift position by one
        return len != -1; // if eof, that's !
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void close() throws IOException { in.close(); }

    @Override
    public float getProgress() throws IOException {
        return ( pos/endOffset )*100;
    }
}

