package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import java.io.*;

public class SuperLongLineReader implements Closeable {

    private FSDataInputStream in;
    private long blockSize, startOffset;
    private int workingBlockSize, posRelativeToSplit=0;
    private byte[] buffer;

    public SuperLongLineReader(FSDataInputStream in, long startOffset, long blockSize) {
        this.in = in;
        this.blockSize = blockSize;
        this.startOffset = startOffset;
        workingBlockSize = (int) blockSize+99;

        System.out.println("working block size: " + workingBlockSize);
        System.out.println("processing block with start offset: " + startOffset);

        buffer = new byte[workingBlockSize];
        try {
            allocateSplitLocally();
        } catch (IOException e) { System.err.println(e); }
    }

    private void allocateSplitLocally() throws IOException  {
        in.seek(startOffset);
        int pos=0;
        while(pos<buffer.length) {
            in.read(buffer, pos, 1);
            pos++;
        }
    }

    public int read(Text value) throws IOException {
        value.set( buffer, posRelativeToSplit, Math.min(100, (buffer.length) - posRelativeToSplit) );

        //System.out.println( "read value : " + value );
        //System.out.println("read with len : " + ((buffer.length) - posRelativeToSplit));
        //System.out.println("pos rel : " + posRelativeToSplit);

        if(posRelativeToSplit % (100000) == 0) {
            System.out.println("done @ " + posRelativeToSplit);
        }

        posRelativeToSplit++;
        // If read to EOF, reset pos to 0
        if(buffer.length-99 - posRelativeToSplit == 0) {
            System.out.println("reach eof at offset: " + (posRelativeToSplit-1));
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
