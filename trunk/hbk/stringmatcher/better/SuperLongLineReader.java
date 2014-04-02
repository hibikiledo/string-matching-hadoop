package hbk.stringmatcher.better;

import org.apache.hadoop.io.BytesWritable;

import java.io.*;

public class SuperLongLineReader implements Closeable {

    private InputStream in;
    private long startOffset, blockSize;
    private int maxPatLength;
    private int posRelativeToSplit = 0;
    private byte[] source;

    public SuperLongLineReader(InputStream in, long startOffset, long blockSize, int maxPatLength) {
        this.in = in;
        this.startOffset = startOffset;
        this.blockSize = blockSize;
        this.maxPatLength = maxPatLength;

        // debug
        System.out.println("LineRecordReader(StartOffset): "+startOffset);
        System.out.println("BlockSize: " + blockSize);
        System.out.println("MaxPatternLength: " + maxPatLength);

        try {
            allocateIntoMem();
        } catch (IOException e) { System.err.println(e); }
    }

    // Allocate the into memory ( only the interest part, not the entire file )
    private void allocateIntoMem() throws IOException  {

        int sourceReadOffset=0;
        int len=0;

        source = new byte[ (int) blockSize + (maxPatLength-1) ];
        System.out.println("source size = " + source.length);

        in.skip( startOffset ); // discard any data before the specify offset
        // Do the reading into mem
        while( sourceReadOffset < source.length-1 ) {
            len = in.read(source, sourceReadOffset, 1);
            if(len==-1) break; // if reach EOF before ideal length, stop the loop
            sourceReadOffset++;
        }
        // debug : to check if it read properly
        System.out.println("Data read from stream: " + sourceReadOffset);

    }

    public int read(BytesWritable valueIn) throws IOException{

        valueIn.set(source, posRelativeToSplit, maxPatLength);
        // System.out.println("Position: " + posRelativeToSplit);
        posRelativeToSplit++;

        return posRelativeToSplit <= (source.length-1)-maxPatLength ? 0 : -1;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
