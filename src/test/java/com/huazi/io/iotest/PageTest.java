//package com.huazi.io.iotest;
//
//import com.huazi.io.iotest.btree.header.FileHeader;
//import com.huazi.io.iotest.btree.header.PageHeader;
//import com.huazi.io.iotest.btree.page.Page;
//import com.huazi.io.iotest.util.Primitives;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//
//public class PageTest {
//    @Test
//    public void pageTest() throws IOException {
//        FileHeader fileHeader = new FileHeader(new File("D:/test.txt"));
//        fileHeader.write();
//        Page page = new Page(fileHeader,5);
//        page.initPage();
//        for(long i=0;i<100;i++){
//            byte[] bytes = Primitives.toBytes(i);
//            page.setBuffer(bytes);
//        }
//        page.write();
//        long[] l = page.readPageData();
//        System.out.println(5);
//    }
//}
