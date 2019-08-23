package com.huazi.io.iotest;

import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btree.header.PageHeader;
import com.huazi.io.iotest.btree.page.Page;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PageTest {
    @Test
    public void pageTest() throws IOException {
        FileHeader fileHeader = new FileHeader(new File("/home/jhren/test.txt"));
        fileHeader.write();
        PageHeader pageHeader = new PageHeader(fileHeader);
        for(long i=0;i<100;i++){
            byte[] bytes = new byte[8];
            bytes[0]=(byte)((i >>> 56) & 0xF);
            bytes[1]=(byte)((i >>> 48) & 0xF);
            bytes[2]=(byte)((i >>> 40) & 0xF);
            bytes[3]=(byte)((i >>> 32) & 0xF);
            bytes[4]=(byte)((i >>> 24) & 0xF);
            bytes[5]=(byte)((i >>> 16) & 0xF);
            bytes[6]=(byte)((i >>>  8) & 0xF);
            bytes[7]=(byte)((i >>>  0) & 0xF);
            pageHeader.setBuffer(bytes);
        }
        pageHeader.write();
        long[] l = pageHeader.readData(1);
        System.out.println(l);
    }
}
