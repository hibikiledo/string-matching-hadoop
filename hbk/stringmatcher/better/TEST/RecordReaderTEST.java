package hbk.stringmatcher.better.TEST;

import hbk.stringmatcher.better.SuperLongRecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.FileInputStream;
import java.io.IOException;

public class RecordReaderTEST {

    public static void main(String[] args) throws IOException{

        FileInputStream is = new FileInputStream("RecordReaderTest.txt");
        // Test with startOffset=5,blockSize=5,patLength=7
        SuperLongRecordReader recordReader = new SuperLongRecordReader(is, 5, 5, 7);
        LongWritable key = new LongWritable();
        BytesWritable value = new BytesWritable();

        while(recordReader.next( key, value )) {
            System.out.println(key.get()+":"+value.getBytes());
        }

    }

}
