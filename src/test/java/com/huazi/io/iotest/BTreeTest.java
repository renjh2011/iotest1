package com.huazi.io.iotest;

import com.huazi.io.iotest.btree.Btree;
import com.huazi.io.iotest.btree.header.FileHeader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BTreeTest {
    @Test
    public void btreeTest() throws IOException {
        FileHeader fileHeader = new FileHeader(new File("D:/test.txt"));
        Btree btree = new Btree(fileHeader);
        btree.init();
//        for(int i = 0;i<100000000;i++) {
//            btree.add(i, i);
//        }
//        btree.getRootNode().write();
//        btree.getNodeLruCache().values().forEach((node)->{
//            try {
//                node.write();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        fileHeader.setHasRoot((byte) 1);
//        fileHeader.write();
        for(int i = 0;i<100000000;i++) {
            if(258573==i)
            System.out.println(i);
            long result =btree.find(i);
            Assert.assertEquals(i,result);
        }
    }
}
