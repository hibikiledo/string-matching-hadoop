package hbk.stringmatcher.better2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class SuperLongRecordReader implements RecordReader<LongWritable, BytesWritable> {

    private long pos;
    private long endOffset;
    private SuperLongLineReader in;
    private int len;
    private boolean isDone = false;

    // Constructor
    public SuperLongRecordReader(FSDataInputStream is, long startOffSet, long blockSize, int maxPatLength) {
        in = new SuperLongLineReader(is, startOffSet, blockSize, maxPatLength);
        pos = startOffSet;
        endOffset = startOffSet + blockSize + (maxPatLength-1);
    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {

        if(isDone)  return false;

        key.set(pos);
        len = in.read(value);
        pos++;  // Each call of this method, shift position by one

        if(len==-1) {
            isDone = true;
        }

        return true;
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

