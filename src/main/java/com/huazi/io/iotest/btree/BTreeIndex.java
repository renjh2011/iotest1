package com.huazi.io.iotest.btree;

import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btreeindex.DataFileHeader;
import com.huazi.io.iotest.btreeindex.DataPage;
import com.huazi.io.iotest.btreeindex.Message;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class BTreeIndex extends Btree {
    private DataFileHeader dataFileHeader;
    DataPage dataPage;
    public BTreeIndex(FileHeader fileHeader,DataFileHeader dataFileHeader,boolean isRead) {
        super(fileHeader);
        this.dataFileHeader = dataFileHeader;
        this.dataPage = getDataPage(isRead);
    }

    public void add(long key, Message message) throws IOException {
        long ptr = store(message);
        add(key,ptr);
    }

    private long store(Message message) throws IOException {
        //如果一页不够存储
        if(!dataPage.setMessage(message)){
            dataPage.write();
            dataPage = getDataPage(false);
            dataPage.setMessage(message);
        }
        return getPtr(dataPage);
    }

    public Message findMsg(long key) throws IOException {
        long ptr = find(key);
        Message message = getMessage(ptr);
        return message;
    }

    private Message getMessage(long ptr) throws IOException {
        short dataIndex = (short) (ptr>>48);
        long pageNum = ptr & 0x0000ffffffffffffL;
        DataPage dataPage = getDataPage(pageNum,true);
        dataPage.getDatas();
        List<Message> messageList = dataPage.getMessageList();
        return messageList.get(dataIndex-1);
    }

    /**
     * 高二位存储的是数据在每一页的偏移量（及第几个）
     * @param dataPage
     * @return
     */
    public long getPtr(DataPage dataPage){
        long pageNum = dataPage.getPageNum();
        long dataIndex = dataPage.getDataIndex();
        //lDataIndex xx000000000000000000000000
        //pageNum    00000000000000000000000xxx
        dataIndex = dataIndex << 48;
        long ptr = dataIndex | pageNum;
        return ptr;
    }
    private DataPage getDataPage(boolean isRead){
        dataPage = new DataPage(dataFileHeader,dataFileHeader.getTotalPage());
        dataPage.init(isRead);
        return dataPage;
    }
    private DataPage getDataPage(long pageNum,boolean isRead){
        dataPage = new DataPage(dataFileHeader,pageNum);
        dataPage.init(isRead);
        return dataPage;
    }

    public DataPage getDataPage() {
        return dataPage;
    }
}
