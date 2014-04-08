package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.*;

public class SuperLongOutputFormat<K,V> extends FileOutputFormat<K,V> {

    protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
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

            if( o instanceof SuperLongValueWrapper) {

                SuperLongValueWrapper v = (SuperLongValueWrapper) o;

                int start = v.getFirstChunkIndex();
                int end = v.getLastChunkIndex();

                if(!v.isExceed()) { // no need to read from chunks
                    String valueStr = v.getValue();
                    for(int i=0; i<valueStr.length(); i++) {
                        out.write( valueStr.charAt(i) );
                    }
                } else { // there is/are chunk(s)

                    System.out.println("Doing from " + start + ">" + end);

                    // write those in file as well
                    for(int i=start; i<=end; i++) {
                        FileInputStream is = new FileInputStream(String.valueOf(i));
                        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                        String chunkData = reader.readLine();
                        for(int j=0; j<chunkData.length(); j++) {
                            out.write(chunkData.charAt(j));
                        }
                    }

                    // also get some portion of left in string builder as well
                    String valueStr = v.getValue();
                    for(int i=0; i<valueStr.length(); i++) {
                        out.write( valueStr.charAt(i) );
                    }
                }
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
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(newline);
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
