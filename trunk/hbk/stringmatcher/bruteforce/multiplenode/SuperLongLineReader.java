package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.Text;

import java.io.*;

public class SuperLongLineReader implements Closeable {

    private InputStream in;
    private RandomAccessFile raf;
    private int len;
    private byte[] buffer = new byte[1024];
    private static final String FILE_NAME = "split_temp";

    public SuperLongLineReader(InputStream in) {
        this.in = in;
        try {
            allocateSplitLocally();
            raf = new RandomAccessFile(new File(FILE_NAME), "r");
        } catch (IOException e) { System.err.println(e); }
    }

    // This improves seek() performance
    private void allocateSplitLocally() throws IOException  {

        FileOutputStream fos = new FileOutputStream(FILE_NAME);
        int readLength;
        while( (readLength = in.read(buffer, 0, buffer.length))!= -1 ) {
            fos.write(buffer,0, readLength);
            fos.flush();
        }

        fos.close();
    }

    public int readAtPos(Text value, long pos, int wordLength) throws IOException {

        raf.seek(pos); // Seek to pos

        byte[] buffer = new byte[ wordLength ];
        len = raf.read(buffer,0, buffer.length);

        value.set( buffer );
        return len;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
