package hbk.stringmatcher.util;

import java.io.*;

public class FileCutter {

    // args[0] : filename
    // args[1] : start (byte)
    // args[2] : end (byte)

    public static void main(String[] args) throws Exception{

        RandomAccessFile inFile = new RandomAccessFile(args[0], "r");
        File outFile = new File(args[1]+"-"+args[2]);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outFile), true);
        inFile.seek(Integer.parseInt(args[1]));

        long targetByteLength = Integer.parseInt(args[2]) - Integer.parseInt(args[1]);
        long fill = 0;

        byte[] buffer = new byte[5];
        while(  (fill += inFile.read(buffer, 0, buffer.length)) <= targetByteLength) {
            writer.write(new String(buffer));
        }

        writer.close();

    }

}
