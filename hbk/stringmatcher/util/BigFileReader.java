package hbk.stringmatcher.util;

import java.io.*;

public class BigFileReader {

    // args[0] : filename

    public static void main(String[] args) throws IOException{

        RandomAccessFile fileIn = new RandomAccessFile(args[0], "r");
        byte[] buffer = new byte[1024];
        while( fileIn.read(buffer) != -1 ) {

            System.out.print( new String(buffer) );

        }

    }

}
