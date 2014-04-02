package hbk.stringmatcher.better;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.ArrayList;

public class SuperLongValueWritable implements Writable, WritableComparable<SuperLongValueWritable>, Comparable<SuperLongValueWritable> {

    // Define hard limit for string builder length to prevent heap error
    public static final int HARD_LIMIT_SB_SIZE = 16777216;

    private String key;
    private StringBuilder value;
    private int currentChunkIndex = 0;

    public void setKet(String keyIn) {
        key = keyIn;
    }

    public StringBuilder appendValue(long valueIn) {
        appendValue(String.valueOf(valueIn));
        return value;
    }


    public StringBuilder appendValue(String valueIn) {

        PrintWriter writer = null;

        if(value.length()+valueIn.length() < HARD_LIMIT_SB_SIZE) { // still less than limit, simply add
            if(value.length() == 0) // first element
                value.append(valueIn);
            else
                value.append(',').append(valueIn);
        } else { // exceed limit
            try {
                // write data in string builder to file
                File chunkOut = new File(String.valueOf(currentChunkIndex));
                writer = new PrintWriter(new FileOutputStream(chunkOut), true);
                writer.println( value.toString() );
                // Increment index
                currentChunkIndex++;
                // Allocate new string builder and appen the data
                value = new StringBuilder();
                value.append(valueIn);
            } catch (IOException e) {
                System.err.println(e);
            } finally {
                if(writer!=null) writer.close();
            }
        }
        return value;
    }

    @Override
    public int compareTo(SuperLongValueWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        if(currentChunkIndex==0) { // no need to read from chunks
            String valueStr = value.toString();
            for(int i=0; i<valueStr.length(); i++) {
                dataOutput.write( valueStr.charAt(i) );
            }
        } else { // there is/are chunk(s) .. have to read chunk(s) first, and write them
            for(int i=0; i<currentChunkIndex; i++) {
                FileInputStream is = new FileInputStream(String.valueOf(i));
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String chunkData = reader.readLine();
                for(int j=0; j<chunkData.length(); j++) {
                    dataOutput.write(chunkData.charAt(j));
                }
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // There is no class having this class as Input
        // No implementation here is needed.
    }
}
