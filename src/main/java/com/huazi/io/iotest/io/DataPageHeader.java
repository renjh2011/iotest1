package com.huazi.io.iotest.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 页头|数据长度（short）
 */
public class DataPageHeader {
    private ByteBuffer pageBuffer = ByteBuffer.allocate(4096);
    private DataFileHeader fileHeader;
    private FileChannel inChannel;
    private FileChannel outChannel;
    /**
     * pageHeaderSize占用每页前两个字节 记录数据长度
     */
    private byte pageHeaderSize = Short.SIZE/8;


    public DataPageHeader(DataFileHeader fileHeader) {
        this.fileHeader = fileHeader;
        inChannel = fileHeader.inChannel;
        outChannel = fileHeader.outChannel;
    }

    public void init(){
        //留出两个字节记录数据个数
        pageBuffer.position(2);
    }
    public void setPageBuffer(byte[] bytes){
        pageBuffer.put(bytes);
    }

    public void write() throws IOException {
        short len = (short) (pageBuffer.position()-pageHeaderSize);
        pageBuffer.rewind();
        pageBuffer.putShort(len);
        pageBuffer.rewind();
        outChannel.write(pageBuffer);
        fileHeader.incrTotalPage();
//        pageBuffer.rewind();
//        init();
    }

    public void read(long pageNum) throws IOException {
        long position = pageNum*4096;
        inChannel.position(position);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
        inChannel.read(byteBuffer);
        byteBuffer.rewind();
        int dataLen = byteBuffer.getShort();
        int i=0;
        int dataCount = dataLen/4;
        while (i<dataCount){
            System.out.println(byteBuffer.getInt());
            i++;
        }
    }

    public int leftCapacity(){
        return pageBuffer.capacity()-pageBuffer.position();
    }

}
