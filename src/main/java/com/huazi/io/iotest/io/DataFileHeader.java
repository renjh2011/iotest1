package com.huazi.io.iotest.io;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

public class DataFileHeader {
    /**
     * fileheader占用第一页
     */
    private short fileHeaderSize = 4096;
    private long totalPage=0;
    private File file;
    protected FileChannel inChannel;
    protected FileChannel outChannel;

    public DataFileHeader(File file) {
        this.file = file;
        if(!this.file.exists()){
            try {
                this.file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try{
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            outChannel = fileOutputStream.getChannel();
            inChannel = FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE,StandardOpenOption.READ,StandardOpenOption.CREATE);;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void incrTotalPage(){
        totalPage++;
    }

    public long getTotalPage() {
        return totalPage;
    }
}
