package hbk.stringmatcher.better;

import org.apache.hadoop.io.BytesWritable;

import java.io.*;

public class SuperLongLineReader implements Closeable {

    private InputStream in;
    private RandomAccessFile raf;
    private long startOffset, blockSize;
    private int maxPatLength, inLength;
    private int posRelativeToSplit = 0;
    private static final String FILE_NAME = "split_temp";

    public SuperLongLineReader(InputStream in, long startOffset, long blockSize, int maxPatLength) {
        this.in = in;
        this.startOffset = startOffset;
        this.blockSize = blockSize;
        this.maxPatLength = maxPatLength;

        System.out.println("LineRecordReader(StartOffset): "+startOffset);

        try {
            allocateSplitLocally();
            raf = new RandomAccessFile(new File(FILE_NAME), "r");
        } catch (IOException e) { System.err.println(e); }
    }

    // Allocate the file locally ( only the interest part, not the entire file )
    private void allocateSplitLocally() throws IOException  {
        int inSize;

        in.skip( startOffset ); // discard any data before the specify offset
        FileOutputStream fos = new FileOutputStream(FILE_NAME); // prepare to write file

        int sizeToBeWritten = (int) blockSize + (maxPatLength-1);
        byte[] buffer = new byte[4096];
        while( sizeToBeWritten > 0) {
            inSize = in.read(buffer, 0, Math.min(sizeToBeWritten, 4096)); // read to the end offset
            if(inSize!=-1) {
                sizeToBeWritten -= inSize;
                fos.write(buffer, 0, inSize); // prevent from writing null character to the file
                System.out.println("allocating left:" + sizeToBeWritten);
            } else {
                break;
            }
        }
        fos.close();
    }

    public int read(BytesWritable valueIn) throws IOException{
        raf.seek( posRelativeToSplit ); // move pointer to pos

        System.out.println("read at pos:" + posRelativeToSplit);

        byte[] buffer = new byte[ maxPatLength ]; // create buffer with the max size of pattern length

        inLength = raf.read(buffer, 0, maxPatLength);
        if (inLength!= -1) valueIn.set(buffer, 0, maxPatLength);

        System.out.println("read value " + new String(valueIn.getBytes()).trim());
        // System.out.println("read length " + inLength);

        posRelativeToSplit++;

        return inLength; // simply return length read, eof will be check in mapper
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
