package com.huazi.io.iotest;

import com.huazi.io.iotest.btree.BTreeIndex;
import com.huazi.io.iotest.btree.Btree;
import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btreeindex.DataFileHeader;
import com.huazi.io.iotest.btreeindex.Message;
import com.huazi.io.iotest.util.Primitives;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BTreeIndexTest {
    @Test
    public void test1() throws IOException {
        long start = System.currentTimeMillis();
        FileHeader fileHeader = new FileHeader(new File("D:/test.txt"));
        DataFileHeader dataFileHeader = new DataFileHeader(new File("D:/data.txt"));
        BTreeIndex bTreeIndex = new BTreeIndex(fileHeader,dataFileHeader,false);
        bTreeIndex.init();
        for(long i=0;i<100000000;i++){
            Message message = new Message(i,i,Primitives.toBytes((long)i));
            bTreeIndex.add(i,message);
        }
        bTreeIndex.getDataPage().write();
        fileHeader.write();
        dataFileHeader.write();
        System.out.println(System.currentTimeMillis()-start);

        for(long i=0;i<10000000;i++){
            Message message = bTreeIndex.findMsg(i);
            Assert.assertEquals(i,message.getT());
        }

    }
}
