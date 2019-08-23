package com.huazi.io.iotest.btree.page;

import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btree.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huazi.io.iotest.Constant.DEFAULT_PAGE_SIZE;

public final class Page implements Comparable<Page> {
        private long pageNum;
        private FileHeader fileHeader;
        private PageHeader pageHeader;
        private FileChannel fileChannel;
        /**
         * The offset into the file that this page starts
         */
        private long pageOffset;

        /**
         * The data for this page
         */
        private ByteBuffer pageData = null;
        /**
         * The position (relative) of the Data in the data array
         */
        private int dataPos;

    public Page(FileHeader fileHeader, PageHeader pageHeader,long pageNum) throws IOException {
        this.fileHeader = fileHeader;
        this.pageHeader = pageHeader;
        this.fileChannel = this.fileHeader.getFileChannel();
        pageData = ByteBuffer.allocate(DEFAULT_PAGE_SIZE);
    }

        /*public synchronized void read() throws IOException {
            if (pageData == null) {
                System.out.println("read in page#" + pageNum + " from page offset " + pageOffset);
                byte[] buf = new byte[bTreeFileHeader.getPageSize()];
                raf.seek(pageOffset);
                raf.read(buf);
                this.pageData = ByteBuffer.wrap(buf);
                bTreePageHeader.read(pageData);
                this.dataPos = bTreeFileHeader.getPageHeaderSize();
            }
        }

        public synchronized void write() throws IOException {
            pageData.rewind();
            bTreePageHeader.write(pageData);
            raf.seek(pageOffset);
            raf.write(pageData.array());
        }

        public void writeData(OutputStream os) throws IOException {
            if (bTreePageHeader.getDataLen() > 0) {
                byte[] b = new byte[bTreePageHeader.getDataLen()];
                pageData.position(dataPos);
                pageData.get(b);
                os.write(b);
            }
        }

        public void readData(InputStream is) throws IOException {
            int avail = is.available();
            int datalen = bTreeFileHeader.getAvalibleSize();
            if (avail < datalen) {
                datalen = avail;
            }
            bTreePageHeader.setDataLen(datalen);
            // read data from stream
            if (datalen > 0) {
                byte[] b = new byte[datalen];
                is.read(b);
                ((Buffer) pageData).position(dataPos);
                pageData.put(b);
            }
        }

        *//**
         * 刷数据
         *
         * @throws IOException
         *//*
        public void flush() throws IOException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("write out page#" + pageNum + " to page offset " + pageOffset);
            }
            raf.seek(pageOffset);
            raf.write(pageData.array());
        }

        protected void initPage() {
            bTreePageHeader.setNextPage(NO_PAGE);
            bTreePageHeader.setStatus(UNUSED);
        }

        public BTreePageHeader getbTreePageHeader() {
            return bTreePageHeader;
        }

        public long getPageNum() {
            return pageNum;
        }*/

        @Override
        public String toString() {
            return "page#" + pageNum;
        }

        @Override
        public int compareTo(Page page) {
            return (int) (pageNum - page.pageNum);
        }
    }