package hbk.stringmatcher.better;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;


import java.io.*;
import java.nio.ByteBuffer;

public class SuperLongOutputFormat<K,V> extends FileOutputFormat<K,V> {

    protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        private static String latestKey=null;
        private static String v;
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        public LineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        private void writeObject(Object o) throws IOException {

            if( o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            }

            if( o instanceof LongWritable) {
                v = String.valueOf(((LongWritable)o).get());
                out.write(v.getBytes(), 0, v.length());
            }
        }

        public synchronized void write(K key, V value)
                throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                // handle first k,v
                if(latestKey==null) {
                    writeObject(key);
                    out.write(keyValueSeparator);
                    writeObject(value);
                    latestKey = key.toString();
                    return;
                }
                if(!key.toString().equals(latestKey)) {
                    out.write(newline);
                    writeObject(key);
                    latestKey = key.toString();
                }
            }
            if (!(nullKey || nullValue)) {
                if(!key.toString().equals(latestKey))
                    out.write(keyValueSeparator);
            }
            if (!nullValue) {
                out.write(keyValueSeparator);
                writeObject(value);
            }
        }

        public synchronized void close(Reporter reporter) throws IOException {
            out.close();
        }
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        // Set kay-value separator to ',' instead of '\t'
        String keyValueSeparator = ",";

        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FileSystem fs = file.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(file, progress);

        return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    }
}