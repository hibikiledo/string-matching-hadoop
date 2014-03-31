package hbk.stringmatcher.better.TEST;

import hbk.stringmatcher.better.BytesArrayWrapper;

public class ByteArrayWrapperTEST {

    public static void main(String[] args) {

        BytesArrayWrapper b = new BytesArrayWrapper();
        byte[] bIn1 = new byte[]{'a','b'};
        byte[] bIn2 = new byte[]{'c','d'};
        byte[] bIn3 = new byte[]{'a','b','c'};

        b.add(bIn1);
        b.add(bIn2);
        b.add(bIn3);

        System.out.println(b.size());
        System.out.println(b.getMaxSize());
        System.out.println(b.getMinSize());

    }

}
