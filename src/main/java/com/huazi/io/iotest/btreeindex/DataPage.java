package com.huazi.io.iotest.btreeindex;

import com.huazi.io.iotest.btree.header.FileHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.huazi.io.iotest.Constant.DEFAULT_PAGE_SIZE;
import static com.huazi.io.iotest.Constant.NO_PAGE;

public class DataPage<T> {
    protected int writePosition;
    protected DataFileHeader dataFileHeader;
    protected FileChannel fileChannel;
    /**
     * 每页的大小
     */
    protected ByteBuffer byteBuffer;
    /**
     *
     */
    public final static short pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 存储数据长度 单位 字节
     * 每条数据占用 8字节
     */
    protected byte dataLenPerCount = 8;
    /**
     *     2 |8 | 8 | data
     *     headZone | dataZone
     */
    public final static byte headSize = 3;
    /**
     * 每页数据占用的空间
     */
    private long pageNum;
    private long filePosition;
    private long pageOffset;

    private boolean pageLoad = false;
    List<Message> messageList;
    //------pageHeader----
    private short dataLen = 0;
    /**
     * 每个message在这一页的第几个位置
     */
    private short dataIndex=0;
//    private long nextPage = NO_PAGE;
//    private long prevPage = NO_PAGE;
//    private long parentPage = NO_PAGE;


    public DataPage(DataFileHeader dataFileHeader,long pageNum) {
        fileChannel = dataFileHeader.fileChannel;
        byteBuffer = ByteBuffer.allocate(pageSize);
        this.dataFileHeader = dataFileHeader;
        this.pageNum = pageNum;
        filePosition=pageNum*pageSize;
    }

    public void setDataLenPerCount(byte dataLenPerCount){
        this.dataLenPerCount = dataLenPerCount;
    }
    public void init(boolean isRead){
        byteBuffer.position(headSize);
        writePosition = headSize;
        if(!isRead) {
            dataFileHeader.incrTotalPage();
        }
    }

    public void setByteBuffer(byte[] bytes){
        byteBuffer.position(this.writePosition);
        byteBuffer.put(bytes);
        this.writePosition = byteBuffer.position();
        dataLen = (short) ((byteBuffer.position()-headSize)/dataLenPerCount);
    }

    public boolean setMessage(Message message){
        long t = message.getT();
        long a = message.getA();
        byte[] body = message.getBody();
        int byteLen = 16+body.length;
        if(writePosition+byteLen>pageSize){
            return false;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteLen);
        byteBuffer.putLong(t);
        byteBuffer.putLong(a);
        byteBuffer.put(body);
        byte[] msg = byteBuffer.array();
        dataLenPerCount = (byte) msg.length;
        setByteBuffer(msg);
        dataIndex++;
        return true;
    }

    public void write() throws IOException {
        byteBuffer.rewind();
        byteBuffer.put(dataLenPerCount);
        byteBuffer.putShort(dataLen);
        byteBuffer.rewind();
        fileChannel.position(filePosition);
        fileChannel.write(byteBuffer);
    }

    public void read() throws IOException {
        fileChannel.position(filePosition);
        byteBuffer.rewind();
        fileChannel.read(byteBuffer);
    }

    public void getDatas() throws IOException {
        read();
        messageList = new ArrayList<>(2<<10);
        byteBuffer.rewind();
        dataLenPerCount = byteBuffer.get();
        dataLen=byteBuffer.getShort();
        for(int i=0;i<dataLen;i++){
            long t =byteBuffer.getLong();
            long a = byteBuffer.getLong();
            byte[] body = new byte[dataLenPerCount-16];
            byteBuffer.get(body);
            Message message = new Message(t,a,body);
            messageList.add(message);
        }
    }

    public List<Message> getMessageList() {
        return messageList;
    }

    public long getPageNum() {
        return pageNum;
    }

    public short getDataIndex() {
        return dataIndex;
    }
}
