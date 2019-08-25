package com.huazi.io.iotest.btree.header;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.huazi.io.iotest.Constant.DEFAULT_PAGE_SIZE;

public class FileHeader {
    private short fileHeaderSize = DEFAULT_PAGE_SIZE;
    private long totalPage=0;
    private long rootPage=1;
    /**
     * root是否存入磁盘 0 false
     */
    private byte hasRoot=0;
    /**
     * 文件头存储总页数与根页
     */
    private byte fileHeaderLen = 16;
    private File file;
    protected FileChannel fileChannel;

    public FileHeader(File file) {
        boolean isNewFile = false;
        this.file = file;
        if(!this.file.exists()){
            try {
                this.file.createNewFile();
                isNewFile = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try{
            fileChannel = FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE,StandardOpenOption.READ,StandardOpenOption.CREATE);
            if(isNewFile){
                totalPage++;
                write();
            }else {
                load();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write() throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
        byteBuffer.putLong(totalPage);
        byteBuffer.putLong(rootPage);
        byteBuffer.put(hasRoot);
        byteBuffer.rewind();
        fileChannel.position(0);
        fileChannel.write(byteBuffer);
    }

    public void load() throws IOException {
        fileChannel.position(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
        fileChannel.read(byteBuffer);
        byteBuffer.rewind();
        totalPage = byteBuffer.getLong();
        rootPage = byteBuffer.getLong();
        hasRoot = byteBuffer.get();
    }

    public byte getHasRoot() {
        return hasRoot;
    }

    public void setHasRoot(byte hasRoot) {
        this.hasRoot = hasRoot;
    }

    public void setRootPage(long pageNum){
        this.rootPage = pageNum;
    }

    public long getRootPage() {
        return rootPage;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public short getFileHeaderSize() {
        return fileHeaderSize;
    }

    public long getTotalPage() {
        return totalPage;
    }

    public long incrTotalPage() {
        return totalPage++;
    }
}
