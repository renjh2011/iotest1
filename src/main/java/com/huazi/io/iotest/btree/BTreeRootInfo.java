package com.huazi.io.iotest.btree;

public final class BTreeRootInfo {

        private final long page;

        private BTreeRootInfo(long page) {
            this.page = page;
        }

        public long getPage() {
            return page;
        }
    }