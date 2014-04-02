package hbk.stringmatcher.better;

import java.util.ArrayList;
import java.util.Iterator;

public class BytesArrayWrapper {

    private ArrayList<byte[]> b = new ArrayList<byte[]>();
    private int minSize = Integer.MAX_VALUE;
    private int maxSize;

    public BytesArrayWrapper() { } // Default constructor

    public int size() { return b.size(); }

    public int getMinSize() {return minSize;}

    public int getMaxSize() {return maxSize;}

    // Wrap around ArrayList add()
    public void add(byte[] bIn) {
        if(!isContained(bIn)) {
            b.add(bIn);
            minSize = bIn.length < minSize ? bIn.length : minSize;
            maxSize = bIn.length > maxSize ? bIn.length : maxSize;
        }
    }

    public Iterator<byte[]> getIterator() {return b.iterator();}

    // Function to check weather the object already hold the specified byte array or not
    private boolean isContained(byte[] bIn) {
        if( b.size() == 0) return false;

        for(int i=0; i<b.size(); i++) {

            byte[] currentElement = b.get(i);
            if( !compareBytes(currentElement, bIn) ) {
                return false;
            }
        }
        return true;
    }

    // Utility function to compareByte
    private boolean compareBytes(byte[] a, byte[] b) {
        if(a.length != b.length) return false;

        for(int i=0; i<a.length; i++) {
            if(a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

}
