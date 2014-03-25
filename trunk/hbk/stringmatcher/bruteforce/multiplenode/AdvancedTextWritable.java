package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdvancedTextWritable implements Writable, WritableComparable<AdvancedTextWritable>, Comparable<AdvancedTextWritable> {

    // resultCode = 0 : string match its split length
    // resultCode NOT = 0 : string contain some null

    private static final String DELIMITER = ",";

    private String value;
    private int resultCode = 0;
    private long offset;

    // Default constructor
    public AdvancedTextWritable() {    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setResultCode(int resultCode) {
        this.resultCode = resultCode;
    }

    public String getValue() {
        return value;
    }

    public int getResultCode() {
        return resultCode;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i=0; i<value.length(); i++)
            dataOutput.write(value.charAt(i));

        dataOutput.write(',');
        dataOutput.write( resultCode );
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        try {
            String[] rawIns = dataInput.readLine().split(DELIMITER);
            this.value = rawIns[0];
            this.resultCode = Integer.parseInt(rawIns[1]);
        } catch ( IOException e ) {
            System.err.println(e);
        }
    }

    @Override
    public int compareTo(AdvancedTextWritable o) {
        return 0;
    }
}