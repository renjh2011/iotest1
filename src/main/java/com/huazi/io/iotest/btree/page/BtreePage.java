package com.huazi.io.iotest.btree.page;

import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btree.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huazi.io.iotest.Constant.*;

public final class BtreePage  implements Comparable<BtreePage> {
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
    public final static short pageSize = DEFAULT_PAGE_SIZE*2;

    /**
     * 存储数据长度 单位 字节
     * 每条数据占用 8字节
     */
    protected final static byte dataLenPerCount = 8;
    /**
     *     2 |8 | 8 | data
     *     headZone | dataZone
     */
    public final static byte headSize = 29;
    public final static int maxDataLen = (pageSize-headSize)/8/2-1;;
    /**
     * 每页数据占用的空间
     */
    private byte state;
    private long pageNum;
    private long filePosition;
    private long pageOffset;

    private long[] keys = new long[0];
    private long[] ptrs = new long[0];
    private boolean pageLoad = false;
    //------pageHeader----
    private byte type;
    private short dataLen = 0;
    private short pointerLen = 0;
    private long nextPage = NO_PAGE;
    private long prevPage = NO_PAGE;
    private long parentPage = NO_PAGE;
    public BtreePage(FileHeader fileHeader, long pageNum) {
        this.fileHeader = fileHeader;
        fileChannel = this.fileHeader.getFileChannel();
        byteBuffer  = ByteBuffer.allocate(pageSize);

        this.pageNum = pageNum;
        this.filePosition = pageNum*pageSize;
        this.state = PageStatus.NEW_PAGE;
        this.type = LEAF;
        pageOffset = pageNum*pageSize;
        initPage();
    }

    public void initPage(){
        byteBuffer.position(headSize);
        writePosition = (short) byteBuffer.position();
    }

    public void setBuffer(byte[] bytes) {
        byteBuffer.position(writePosition);
        byteBuffer.put(bytes);
        writePosition = (short) byteBuffer.position();
        setLen();
        this.state = PageStatus.USE_PAGE;
    }
    public void setLen(){
        int totalLen = (byteBuffer.position()-headSize);
        int totalCount = totalLen/8;
        if((totalCount & 1)!=0){
            dataLen = (short) (totalCount/2);
            pointerLen = (short) (dataLen+1);
        }else {
            dataLen = (short) (totalCount/2);
            pointerLen = dataLen;
        }
    }

    public void write() throws IOException {
        byteBuffer.rewind();
        byteBuffer.put(type);
        byteBuffer.putShort(dataLen);
        byteBuffer.putShort(pointerLen);
        byteBuffer.putLong(prevPage);
        byteBuffer.putLong(nextPage);
        byteBuffer.putLong(parentPage);
        writePosition = (short) byteBuffer.position();

        fileChannel.position(filePosition);
        byteBuffer.rewind();
        fileChannel.write(byteBuffer);
        this.state = PageStatus.WRITE_PAGE;
    }

    public void getFreePage() throws IOException {
//        long totalPage = fileHeader.getTotalPage();
        fileChannel.position(fileChannel.size());
        fileChannel.position(filePosition);
    }
    public void readPage() throws IOException {
        if(pageOffset>=fileChannel.size()){
            throw new IOException("页码过大");
        }
        if(state!=PageStatus.USE_PAGE){
            fileChannel.position(filePosition);
            byteBuffer.rewind();
            fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            type = byteBuffer.get();
            dataLen = byteBuffer.getShort();
            pointerLen = byteBuffer.getShort();
            prevPage = byteBuffer.getLong();
            nextPage = byteBuffer.getLong();
            parentPage = byteBuffer.getLong();
        }
        byteBuffer.rewind();
    }

    public long[] getKeys(){
        byteBuffer.position(headSize);
        long[] keys = new long[dataLen];
        for(int i = 0;i<keys.length;i++){
            keys[i]=byteBuffer.getLong();
        }
        this.keys=keys;
        return keys;
    }
    public long[] getPtrs(){
        byteBuffer.position(headSize+dataLen*dataLenPerCount);
        long[] ptrs = new long[pointerLen];
        for(int i = 0;i<ptrs.length;i++){
            ptrs[i]=byteBuffer.getLong();
        }
        this.ptrs=ptrs;
        return ptrs;
    }

    /*public long[] readPageData() throws IOException {
        readPage();
        byteBuffer.position(headSize);
//        LongBuffer longBuffer = readBuffer.asLongBuffer();
//        return longBuffer.array();
        long[] longs = new long[dataLen/dataLenPerCount];
        for(int i=0;i<getDataLen()/dataLenPerCount;i++){
            longs[i]=byteBuffer.getLong();
        }
        return longs;
    }*/

    @Override
    public String toString() {
        return "page#" + pageNum;
    }

    @Override
    public int compareTo(BtreePage page) {
        return (int) (pageNum - page.pageNum);
    }

    public long getPageNum() {
        return pageNum;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
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

    public long getPrevPage() {
        return prevPage;
    }

    public void setPrevPage(long prevPage) {
        this.prevPage = prevPage;
    }
}