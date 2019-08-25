package com.huazi.io.iotest.btreeindex;

import com.huazi.io.iotest.Constant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.huazi.io.iotest.Constant.DEFAULT_PAGE_SIZE;

public class DataFileHeader {
    /**
     * fileheader占用第一页
     */
    static short fileHeaderSize = Constant.DEFAULT_PAGE_SIZE;
    private long totalPage=0;
    private File file;
    protected FileChannel fileChannel;

    public DataFileHeader(File file) {
        this.file = file;
        boolean isNewFile = false;
        if(!this.file.exists()){
            try {
                this.file.createNewFile();
                isNewFile=true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try{
            fileChannel = FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE,StandardOpenOption.READ,StandardOpenOption.CREATE);;
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(fileHeaderSize);
        byteBuffer.putLong(totalPage);
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
    }
    public long incrTotalPage(){
        return totalPage++;
    }

    public long getTotalPage() {
        return totalPage;
    }
}
