package hbk.stringmatcher.bruteforce.multiplenode;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.net.NetworkTopology;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

public class SuperLongInputFormat extends TextInputFormat {

    public static final String NUM_INPUT_FILES = org.apache.hadoop.mapreduce.lib.input.FileInputFormat.NUM_INPUT_FILES;
    public static final String INPUT_DIR_RECURSIVE =  org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE;
    private static final double SPLIT_SLOP = 1.1;   // 10% slop
    private long minSplitSize = 1;

    private ArrayList<Integer> reqStrSize = new ArrayList<Integer>();
    private String stringListFileName;

    @Override
    public void configure(JobConf conf) {
        // Call super class method first
        super.configure(conf);
        getReqStrSize(conf);
    }

    private void getReqStrSize(JobConf conf) {
        try {
            URI[] files = DistributedCache.getCacheFiles(conf);
            System.out.println( files[0].getPath() ); // debug
            File stringList = new File( files[0].getPath() );

            BufferedReader reader = new BufferedReader(new InputStreamReader( new FileInputStream( stringList )));
            String line;
            while((line=reader.readLine())!=null) {
                System.out.println("LineReadIn: " + line);
                reqStrSize.add(line.length());
            }

        } catch (IOException e) {
            System.err.println(e);
        }
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        // return new SuperLongLineRecordReader();

        FileSplit fileSplit = null;
        Path filePath;
        FileSystem fileSystem;
        FSDataInputStream fileInputStream;

        if ( genericSplit instanceof FileSplit) {

            fileSplit = (FileSplit) genericSplit;
            filePath = fileSplit.getPath();

            fileSystem = filePath.getFileSystem(job);
            fileInputStream = fileSystem.open(filePath);

            // Convert to int[]
            int[] reqStrSizeInt = new int[ reqStrSize.size() ];
            for(int i=0; i< reqStrSize.size(); i++) {
                reqStrSizeInt[i] = reqStrSize.get(i);
            }

            System.out.println( "Request Size:" + reqStrSize.size() );

            return new SuperLongRecordReader(fileInputStream, reqStrSizeInt, fileSplit.getStart());
        }

        return null;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        FileStatus[] files = listStatus(job);

        // Save the number of input files for metrics/loadgen
        job.setLong(NUM_INPUT_FILES, files.length);
        long totalSize = 0;                           // compute total size
        for (FileStatus file: files) {                // check we have valid files
            if (file.isDirectory()) {
                throw new IOException("Not a file: "+ file.getPath());
            }
            totalSize += file.getLen();
        }

        long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
        long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
                FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

        // generate splits
        ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
        NetworkTopology clusterMap = new NetworkTopology();
        for (FileStatus file: files) {
            Path path = file.getPath();
            long length = file.getLen();
            if (length != 0) {
                FileSystem fs = path.getFileSystem(job);
                BlockLocation[] blkLocations;
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                if (isSplitable(fs, path)) {
                    long blockSize = file.getBlockSize();
                    long splitSize = computeSplitSize(goalSize, minSize, blockSize);

                    long bytesRemaining = length;
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                        String[] splitHosts = getSplitHosts(blkLocations,
                                length-bytesRemaining, splitSize, clusterMap);
                        splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                                splitHosts));
                        bytesRemaining -= splitSize;
                    }

                    if (bytesRemaining != 0) {
                        String[] splitHosts = getSplitHosts(blkLocations, length
                                - bytesRemaining, bytesRemaining, clusterMap);
                        splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                                splitHosts));
                    }
                } else {
                    // if not splitable,, simply make split with the whole file length
                    String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
                    splits.add(makeSplit(path, 0, length, splitHosts));
                }
            } else {
                //Create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
            }
        }
        // Debug - print out detail of each input split
        for( FileSplit split : splits ) {
            System.out.println("Name:" + split.getPath().getName());
            System.out.println("Start:" + split.getStart());
        }
        System.out.println("Split Count: " + splits.size());
        return splits.toArray(new FileSplit[splits.size()]);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }
}

