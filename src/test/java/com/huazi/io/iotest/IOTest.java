package com.huazi.io.iotest;

import com.huazi.io.iotest.io.DataFileHeader;
import com.huazi.io.iotest.io.DataPageHeader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class IOTest {
    @Test
    public void test1() throws IOException {
        DataFileHeader fileHeader = new DataFileHeader(new File("/home/jhren/test.txt"));
        DataPageHeader pageHeader = new DataPageHeader(fileHeader);
        pageHeader.init();

        for(int i = 0;i<200000000;i++){
            byte[] bytes = new byte[4];
            bytes[0]= (byte) ((i >>> 24) & 0xFF);
            bytes[1]= (byte) ((i >>> 16) & 0xFF);
            bytes[2]= (byte) ((i >>>  8) & 0xFF);
            bytes[3]= (byte) ((i >>>  0) & 0xFF);
            if(pageHeader.leftCapacity()<bytes.length){
                pageHeader.write();
                pageHeader=new DataPageHeader(fileHeader);
                pageHeader.init();
            }
            pageHeader.setPageBuffer(bytes);
        }
        pageHeader.write();

        System.out.println(fileHeader.getTotalPage());
//        for(int i=0;i<fileHeader.getTotalPage();i++){
//            pageHeader.rad(i);
////            System.out.println("===============================");
//        }
        pageHeader.read(fileHeader.getTotalPage()-1);
    }
}
