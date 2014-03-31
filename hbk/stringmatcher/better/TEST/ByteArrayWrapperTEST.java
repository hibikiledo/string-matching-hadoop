package hbk.stringmatcher.better.TEST;

import hbk.stringmatcher.better.BytesArrayWrapper;

import java.util.Iterator;

public class ByteArrayWrapperTEST {

    public static void main(String[] args) {

        BytesArrayWrapper b = new BytesArrayWrapper();
        byte[] bIn1 = new byte[]{'a','b'};
        byte[] bIn2 = new byte[]{'a','b'};
        byte[] bIn3 = new byte[]{'a','b','c'};
        byte[] bIn4 = new byte[]{'c','d','e'};

        b.add(bIn1);
        b.add(bIn2);
        b.add(bIn3);
        b.add(bIn4);

        Iterator<byte[]> i = b.getIterator();
        while(i.hasNext()) {
            System.out.println(new String(i.next()));
        }

        System.out.println(b.size());
        System.out.println(b.getMaxSize());
        System.out.println(b.getMinSize());

    }

}
