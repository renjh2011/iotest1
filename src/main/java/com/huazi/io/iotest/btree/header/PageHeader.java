package com.huazi.io.iotest.btree.header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import static com.huazi.io.iotest.Constant.*;

public class PageHeader {
    protected short writePosition;
    protected FileHeader fileHeader;
    protected FileChannel fileChannel;
    /**
     * 每页的大小
     */
    protected ByteBuffer byteBuffer;
    /**
     *
     */
    protected short pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 存储数据长度 单位 字节
     * 每条数据占用 8字节
     */
    protected final static byte dataLenPerCount = 8;
    /**
     *     2 |8 | 8 | data
     *     headZone | dataZone
     */
    protected final static byte headSize = 18;
    /**
     * 每页数据占用的空间
     */
    private short dataLen = 0;
    private long nextPage = NO_PAGE;
    private long parentPage = NO_PAGE;

    public PageHeader(FileHeader fileHeader) {
        this.fileHeader = fileHeader;
        fileChannel = this.fileHeader.fileChannel;
        byteBuffer  = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
    }

    public void initPage(){
        byteBuffer.position(headSize);
        writePosition = (short) byteBuffer.position();
    }

    public void setBuffer(byte[] bytes){
        byteBuffer.position(writePosition);
        byteBuffer.put(bytes);
        writePosition = (short) byteBuffer.position();
        dataLen = (short) (byteBuffer.position()-headSize);
    }

    public void write() throws IOException {
        dataLen = (short) (byteBuffer.position()-headSize);
        byteBuffer.rewind();
        byteBuffer.putShort(dataLen);
        byteBuffer.putLong(nextPage);
        byteBuffer.putLong(parentPage);
        writePosition = (short) byteBuffer.position();
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

    public short getDataLen() {
        return dataLen;
    }

    protected void setDataLen(short dataLen) {
        this.dataLen = dataLen;
    }
}
