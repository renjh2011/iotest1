package com.huazi.io.iotest.btree.header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static com.huazi.io.iotest.Constant.*;

public class PageHeader {
    private FileHeader fileHeader;
    private FileChannel fileChannel;
    /**
     * 每页的大小
     */
    private ByteBuffer byteBuffer;
    /**
     *
     */
    private short pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 存储数据长度 单位 字节
     * 每条数据占用 8字节
     */
    private final static byte dataLenPerCount = 8;
    /**
     *     2 |8 | 8 | data
     *     headZone | dataZone
     */
    private final static byte headSize = 18;
    /**
     * 每页数据占用的空间
     */
    private short dataLen = 0;
    private long nextPage = NO_PAGE;
    private long parentPage = NO_PAGE;

    private byte status = NEW_PAGE;

    public PageHeader(FileHeader fileHeader) {
        this.fileHeader = fileHeader;
        fileChannel = this.fileHeader.fileChannel;
        byteBuffer  = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
    }

    public void initPage(){
        byteBuffer.position(headSize);
    }

    public void setBuffer(byte[] bytes){
        byteBuffer.put(bytes);
    }
    public void write() throws IOException {
        int position = byteBuffer.position();
        dataLen = (short) (byteBuffer.capacity()-position-headSize);
        byteBuffer.rewind();
        byteBuffer.putShort(dataLen);
        byteBuffer.putLong(nextPage);
        byteBuffer.putLong(parentPage);
        byteBuffer.rewind();
        long size = fileChannel.size();
        fileChannel.position(size);
        fileChannel.write(byteBuffer);
    }
    //读取整页
    public ByteBuffer read(long pageNum) throws IOException {
//        long pageOffset = fileHeader.getFileHeaderSize()+(pageNum-1)*pageSize;
        long pageOffset = pageNum*pageSize;
        ByteBuffer readBuffer = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
        fileChannel.position(pageOffset);
        fileChannel.read(readBuffer);
        return readBuffer;
    }

    //读取该页数据区
    public long[] readData(long pageNum) throws IOException {
        ByteBuffer readBuffer = read(pageNum);
        if(readBuffer.capacity()!=DEFAULT_PAGE_SIZE){
            throw new IOException("页数据不完整");
        }
        readBuffer.position(headSize);
        LongBuffer longBuffer = readBuffer.asLongBuffer();
        return longBuffer.array();
    }

    public long getNextPage() {
        return nextPage;
    }

    public void setNextPage(long nextPage) {
        this.nextPage = nextPage;
    }

    public long getParentPage() {
        return parentPage;
    }

    public void setParentPage(long parentPage) {
        this.parentPage = parentPage;
    }
}
