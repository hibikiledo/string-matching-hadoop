package hbk.stringmatcher.better;

import java.util.ArrayList;
import java.util.Iterator;

public class BytesArrayWrapper {

    private ArrayList<byte[]> b = new ArrayList<byte[]>();
    private int minSize = Integer.MAX_VALUE;
    private int maxSize;

    public BytesArrayWrapper() {

    }

    public int size() { return b.size(); }

    public int getMinSize() {return minSize;}

    public int getMaxSize() {return maxSize;}

    public void add(byte[] bIn) {
        if(isContained(bIn)) {
            b.add(bIn);
            minSize = bIn.length < minSize ? bIn.length : minSize;
            maxSize = bIn.length > maxSize ? bIn.length : maxSize;
        }
    }

    public Iterator<byte[]> getIterator() {return b.iterator();}

    private boolean isContained(byte[] bIn) {
        for(byte[] each : b) {
            for(int i=0; i<each.length; i++) {
                if(each[i] != bIn[i])
                    return false;
            }
        }
        return true;
    }

}
