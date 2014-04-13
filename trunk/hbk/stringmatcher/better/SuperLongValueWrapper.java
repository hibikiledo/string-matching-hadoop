package hbk.stringmatcher.better;

import java.io.*;

public class SuperLongValueWrapper {

    // Define hard limit for string builder length to prevent heap error
    public static final int HARD_LIMIT_SB_SIZE = 16777216;

    private StringBuilder value;
    private static int currentChunkIndex = 0;
    private int start,end;
    private boolean isExceed = false;

    public SuperLongValueWrapper() {
        value = new StringBuilder();
        start = currentChunkIndex;
        end = start;
    }

    public StringBuilder appendValue(long valueIn) {
        appendValue(String.valueOf(valueIn));
        return value;
    }

    public int getFirstChunkIndex() {return start;}
    public int getLastChunkIndex() {return end-1;}

    public boolean isExceed() {return isExceed;}

    public StringBuilder appendValue(String valueIn) {

        PrintWriter writer = null;

        if(value.length()+valueIn.length() < HARD_LIMIT_SB_SIZE) { // still less than limit, simply add
            if(value.length() == 0) // first element
                value.append(valueIn);
            else
                value.append(',').append(valueIn);
        } else { // exceed limit
            try {
                isExceed = true;

                // write old data in string builder to file
                File chunkOut = new File(String.valueOf(currentChunkIndex));
                writer = new PrintWriter(new FileOutputStream(chunkOut), true);
                writer.println( value.toString() );
                // Increment index
                currentChunkIndex++;
                end++;
                // Allocate new string builder and append new data
                value = new StringBuilder();
                value.append(',').append(valueIn);

            } catch (IOException e) {
                System.err.println(e);
            } finally {
                if(writer!=null) writer.close();
            }
        }

        return value;
    }

    public String getValue() {
        return value.toString();
    }
}