package com.huazi.io.iotest;

import com.huazi.io.iotest.btreeindex.DataFileHeader;
import com.huazi.io.iotest.btreeindex.DataPage;
import com.huazi.io.iotest.btreeindex.DataStore;
import com.huazi.io.iotest.btreeindex.Message;
import com.huazi.io.iotest.util.Primitives;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class DataStoreTest {
    @Test
    public void test1() throws IOException {
        DataFileHeader dataFileHeader = new DataFileHeader(new File("D:/data.txt"));
//        DataPage dataPage = new DataPage(dataFileHeader,dataFileHeader.getTotalPage());
//        dataPage.init(false);
//        for(int i = 0;i<1000000;i++){
//            Message message = new Message(i,i,Primitives.toBytes((long)i));
//            //如果放不下了
//            if(!dataPage.setMessage(message)){
//                dataPage.write();
//                dataPage = new DataPage(dataFileHeader,dataFileHeader.getTotalPage());
//                dataPage.init(false);
//                dataPage.setMessage(message);
//            }
//        }
//        dataPage.write();
//        dataFileHeader.write();
        DataPage dataPage;
        for(int i=1;i<=10;i++) {
            dataPage = new DataPage(dataFileHeader,i);
            dataPage.init(true);
            dataPage.getDatas();
            List<Message> messages = dataPage.getMessageList();
            System.out.println(messages);
        }
    }
}
