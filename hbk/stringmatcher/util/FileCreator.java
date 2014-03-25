package hbk.stringmatcher.util;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class FileCreator {

    public static void main(String args[]) throws Exception{

        // args[0] file size
        // args[1] file name

        PrintWriter writer = new PrintWriter(new FileOutputStream(args[1]),true);
        char[] chars = new char[4];

        chars[0] = 'a';
        chars[1] = 'b';
        chars[2] = 'c';
        chars[3] = 'd';

        long targetSize = Integer.parseInt(args[0]) * 1024 * 1024;
        long startSize = 0;

        while( startSize < targetSize ) {

            writer.write(chars[(int)( Math.random() * 4 )]);
            startSize+=1;

        }

        // Close output stream
        writer.close();
    }

}


