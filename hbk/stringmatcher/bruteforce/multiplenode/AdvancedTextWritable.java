package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdvancedTextWritable implements Writable, WritableComparable<AdvancedTextWritable>, Comparable<AdvancedTextWritable> {

    private static final String DELIMITER = ",";

    private String value;
    private long offset;

    // Default constructor
    public AdvancedTextWritable() {    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Debug
        System.out.println("Writing from AVT: " + value + ":" + offset);

        for(int i=0; i<value.length(); i++)
           dataOutput.write(value.charAt(i));

        dataOutput.write(',');
        writeOffset(offset, dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        try {
            String line = dataInput.readLine();
            System.out.println("ReadField, in line: " + line); // debug
            String[] rawIns = line.split(DELIMITER);
            System.out.println("value:" + rawIns[0] + "-- offset" + rawIns[1]);
            this.value = rawIns[0];
            this.offset = Integer.parseInt(rawIns[1]);

        } catch ( IOException e ) {
            System.err.println(e);
        }
    }

    @Override
    public int compareTo(AdvancedTextWritable o) {
        return 0;
    }

    private void writeOffset(long offset, DataOutput out)throws IOException{
        String offsetStr = String.valueOf(offset);
        for(int i=0; i<offsetStr.length(); i++) {
            out.write(offsetStr.charAt(i));
        }
    }
}