/*
package com.huazi.io.iotest.btree;

public final class BTreeNode implements Comparable<BTreeNode> {

        private final BTreeRootInfo root;
        private final Page page;
        private final BTreePageHeader bTreePageHeader;

        private BTreeNode parentCache;

        private Data[] keys;
        private long[] ptrs;
        private long next = -1;
        private long prev = -1;

        private boolean loaded = false;
        private int currentDataLen = -1;
        private boolean dirty = false;


        protected BTreeNode(final BTreeRootInfo root, final Page page, final BTreeNode parentNode) {
            this.root = root;
            this.page = page;
            this.bTreePageHeader =  page.getbTreePageHeader();
            bTreePageHeader.setParent(parentNode);
//            long parentNum = parentNode==null ? -1 : parentNode.getPage().getPageNum();
//            bTreePageHeader.setParentPageNum(parentNum);
            this.parentCache = parentNode;
        }

        protected BTreeNode(final BTreeRootInfo root, final Page page) {
            this.root = root;
            this.page = page;
            this.bTreePageHeader = (BTreePageHeader) page.getbTreePageHeader();
        }

        private void setParent(BTreeNode node) {
            long parentPage = node.page.getPageNum();
            if (parentPage != bTreePageHeader.getParentPage()) {
                bTreePageHeader.setParentPage(parentPage);
                this.parentCache = node;
                this.dirty = true;
            }
        }
        private BTreeNode getParent() {
            if (parentCache != null) {
                return parentCache;
            }
            long page = bTreePageHeader.getParentPage();
            if (page != Constant.NO_PAGE) {
                try {
                    parentCache = getBTreeNode(btreeRootInfo, page);
                    //todo 从磁盘重新初始化的root要赋给btreeRootNode
                    if(parentCache.page.getPageNum()==btreeRootNode.page.getPageNum()){
                        btreeRootNode=parentCache;
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("failed to get parent page #" + page, e);
                }
                return parentCache;
            }
            return null;
        }


        long addValue(Data key, final long pointer) throws IOException {
            int idx = BinarySearchUtils.searchRightmostKey(keys, key, keys.length);
            switch (bTreePageHeader.getStatus()) {
                case BRANCH: {
                    idx = idx < 0 ? -(idx + 1) : idx + 1;
                    return getChildNode(idx).addValue(key, pointer);
                }
                case LEAF: {
                    final boolean found = idx >= 0;
                    final long oldPtr;
                    if (found) {
                        if (!isDuplicateAllowed()) {
                            throw new BTreeCorruptException(
                                "Attempt to add duplicate key to the unique index: " + key);
                        }
                        oldPtr = ptrs[idx];
                        key = keys[idx]; // use the existing key object
                        idx = idx + 1;
                    } else {
                        oldPtr = -1;
                        idx = -(idx + 1);
                    }
                    set(ArrayUtils.<Data>insert(keys, idx, key),
                        ArrayUtils.insert(ptrs, idx, pointer));
                    incrDataLength(key, pointer);

                    // Check to see if we've exhausted the block
                    if (needSplit()) {
                        split();
                    }
                    return oldPtr;
                }
                default:
                    throw new BTreeCorruptException("Invalid Page Type '" + bTreePageHeader.getStatus()
                            + "' was detected for page#" + page.getPageNum());
            }
        }


        */
/**
         * Internal (to the BTreeNode) method. Because this method is called only by BTreeNode
         * itself, no synchronization done inside of this method.
         *//*

        private BTreeNode getChildNode(final int idx) throws IOException {
            if (bTreePageHeader.getStatus() == BRANCH && idx >= 0 && idx < ptrs.length) {
                return getBTreeNode(root, ptrs[idx], this);
            }
            return null;
        }

        */
/**
         * Need to split this node after adding one more value?
         * 
         * @see #write()
         *//*

        private boolean needSplit() {
            int afterKeysLength = keys.length + 1;

            if (afterKeysLength < LEAST_KEYS) {
                return false;
            }
            if (afterKeysLength > Short.MAX_VALUE) {
                return true;
            }
            // CurrLength + one Long pointer + value length + one int (for value length)
            // actual datalen is smaller than this datalen, because prefix is used.
            int datalen = calculateDataLength();
            int worksize = bTreeFileHeader.getAvalibleSize();
            return datalen > worksize;
        }

        */
/**
         * Internal to the BTreeNode method
         *//*

        private void split() throws IOException {
            final Data[] leftVals;
            final Data[] rightVals;
            final long[] leftPtrs;
            final long[] rightPtrs;
            final Data separator;

            final short vc = bTreePageHeader.getDataCount();
            int pivot = vc / 2;

            // Split the node into two nodes
            final byte pageType = bTreePageHeader.getStatus();
            switch (pageType) {
                case BRANCH: {
                    leftVals = new Data[pivot];
                    leftPtrs = new long[leftVals.length + 1];
                    rightVals = new Data[vc - (pivot + 1)];
                    rightPtrs = new long[rightVals.length + 1];

                    System.arraycopy(keys, 0, leftVals, 0, leftVals.length);
                    System.arraycopy(ptrs, 0, leftPtrs, 0, leftPtrs.length);
                    System.arraycopy(keys, leftVals.length + 1, rightVals, 0, rightVals.length);
                    System.arraycopy(ptrs, leftPtrs.length, rightPtrs, 0, rightPtrs.length);

                    separator = keys[leftVals.length];
                    break;
                }
                case LEAF: {
                    leftVals = new Data[pivot];
                    leftPtrs = new long[leftVals.length];
                    rightVals = new Data[vc - pivot];
                    rightPtrs = new long[rightVals.length];

                    System.arraycopy(keys, 0, leftVals, 0, leftVals.length);
                    System.arraycopy(ptrs, 0, leftPtrs, 0, leftPtrs.length);
                    System.arraycopy(keys, leftVals.length, rightVals, 0, rightVals.length);
                    System.arraycopy(ptrs, leftPtrs.length, rightPtrs, 0, rightPtrs.length);
                    //todo
                    separator = getSeparator(leftVals[leftVals.length - 1], rightVals[0]);
                    break;
                }
                default:
                    throw new BTreeCorruptException("Invalid page type in split: " + pageType);
            }

            // Promote the pivot to the parent branch
            final BTreeNode parent = getParent(); // this node may be GC'd
            if (parent == null) {
                // This can only happen if this is the root     
                BTreeNode lNode = createBTreeNode(root, pageType, this);
                lNode.set(leftVals, leftPtrs);
                lNode.calculateDataLength();
                lNode.setAsParent();

                BTreeNode rNode = createBTreeNode(root, pageType, this);
                rNode.set(rightVals, rightPtrs);
                rNode.calculateDataLength();
                rNode.setAsParent();

                if (pageType == LEAF) {
                    setLeavesLinked(lNode, rNode);
                }

                bTreePageHeader.setStatus(BRANCH);
                set(new Data[] {separator},
                    new long[] {lNode.page.getPageNum(), rNode.page.getPageNum()});
                calculateDataLength();
            } else {
                set(leftVals, leftPtrs);
                calculateDataLength();

                BTreeNode rNode = createBTreeNode(root, pageType, parent);
                rNode.set(rightVals, rightPtrs);
                rNode.calculateDataLength();
                rNode.setAsParent();

                if (pageType == LEAF) {
                    setLeavesLinked(this, rNode);
                }

                long leftPtr = page.getPageNum();
                long rightPtr = rNode.page.getPageNum();
                parent.promoteValue(separator, leftPtr, rightPtr);
            }
        }

        */
/** Set the parent-link in all child nodes to point to this node *//*

        private void setAsParent() throws IOException {
            if (bTreePageHeader.getStatus() == BRANCH) {
                for (final long ptr : ptrs) {
                    BTreeNode child = getBTreeNode(btreeRootInfo, ptr, this);
                    child.setParent(this);
                }
            }
        }

        */
/** Set leaves linked *//*

        private void setLeavesLinked(final BTreeNode left, final BTreeNode right) throws IOException {
            final long leftPageNum = left.page.getPageNum();
            final long rightPageNum = right.page.getPageNum();
            final long origNext = left.next;
            if (origNext != -1L) {
                right.next = origNext;
                BTreeNode origNextNode = getBTreeNode(root, origNext);
                origNextNode.prev = rightPageNum;
                origNextNode.setDirty(true);
            }
            left.next = rightPageNum;
            right.prev = leftPageNum;
        }

        private void promoteValue(final Data key, final long leftPtr, final long rightPtr) throws IOException {
            final int leftIdx = BinarySearchUtils.searchRightmostKey(keys, key, keys.length);
            int insertPoint = (leftIdx < 0) ? -(leftIdx + 1) : leftIdx + 1;
            boolean found = false;
            for (int i = insertPoint; i >= 0; i--) {
                final long ptr = ptrs[i];
                if (ptr == leftPtr) {
                    insertPoint = i;
                    found = true;
                    break;
                } else {
                    continue; // just for debugging
                }
            }
            if (!found) {
                throw new IllegalStateException(
                    "page#" + page.getPageNum() + ", insertPoint: " + insertPoint + ", leftPtr: "
                            + leftPtr + ", ptrs: " + Arrays.toString(ptrs));
            }
            set(ArrayUtils.<Data>insert(keys, insertPoint, key),
                ArrayUtils.insert(ptrs, insertPoint + 1, rightPtr));
            incrDataLength(key, rightPtr);
            // Check to see if we've exhausted the block
            if (needSplit()) {
                split();
            }
        }

        */
/** Gets shortest-possible separator for the pivot *//*

        //todo String类型为啥可以
        private Data getSeparator(final Data value1, final Data value2) {
//            int idx = value1.compareTo(value2);
            byte[] b = new byte[value2.get_len()];
            value2.copyTo(b, 0, b.length);
            return new Data(b);
           */
/* if (idx == 0) {
                return value1.clone();
            }
            byte[] b = new byte[Math.abs(idx)];
            value2.copyTo(b, 0, b.length);
            return new Data(b);*//*

        }

        */
/**
         * Sets values and pointers. Internal (to the BTreeNode) method, not synchronized.
         *//*

        private void set(final Data[] values, final long[] ptrs) {
            final int vlen = values.length;
            if (vlen > Short.MAX_VALUE) {
                throw new IllegalArgumentException("entries exceeds limit: " + vlen);
            }
            this.keys = values;
            this.ptrs = ptrs;
            this.bTreePageHeader.setDataCount((short) vlen);
            setDirty(true);
            // required for paging out
            cacheMap.put(page.getPageNum(), this);
        }

        private void setDirty(final boolean dirt) {
            this.dirty = dirt;
        }

        private Data getPrefix(final Data v1, final Data v2) {
            final int idx = Math.abs(v1.compareTo(v2)) - 1;
            if (idx > 0) {
                final byte[] d2 = v2.getData();
                return new Data(d2, v2.getPosition(), idx);
            } else {
                return EmptyData;
            }
        }

        */
/**
         * Reads node only if it is not loaded yet
         *//*

        private void read() throws IOException {
            if (!this.loaded) {
                Data v = readData(page);
                DataInputStream in = new DataInputStream(v.getInputStream());
                //todo 从文件中读取pageHeader信息 如果首节点一直缓存或者从磁盘中读取出来后赋给首节点 可以不要page.read1()
//                page.read1();
                // Read in the Values
                Data prevKey = null;
                final int keysLen = bTreePageHeader.getDataCount();
                keys = new Data[keysLen];
                for (int i = 0; i < keysLen; i++) {
                    //读取此页的状态
                    final int len = in.readInt();
                    //与前一个值相同
                    if (len == -1) {
                        prevKey.incrRefCount();
                        keys[i] = prevKey;
                    } else {
                        byte[] b = new byte[len];
                        if (len > 0) {
                            in.read(b, 0, len);
                        }
                        prevKey = new Data(b);
                        keys[i] = prevKey;
                    }
                }
                // Read in the pointers
                final int ptrslen = bTreePageHeader.getPointerCount();
                ptrs = new long[ptrslen];
                for (int i = 0; i < ptrslen; i++) {
                    ptrs[i] = VariableByteCodec.decodeUnsignedLong(in);
                }
                // Read in the links if current node is a leaf
                if (bTreePageHeader.getStatus() == LEAF) {
                    this.prev = in.readLong();
                    this.next = in.readLong();
                }
                this.currentDataLen = v.getLength();
                this.loaded = true;

            }
        }

        public void write() throws IOException {
            if (!dirty) {
                return;
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace((bTreePageHeader.getStatus() == LEAF ? "Leaf " : "Branch ") + "Node#"
                        + page.getPageNum() + " - " + Arrays.toString(keys));
            }
            final FastMultiByteArrayOutputStream bos =
                    new FastMultiByteArrayOutputStream(bTreeFileHeader.getAvalibleSize());
            final DataOutputStream os = new DataOutputStream(bos);

            // write out the prefix
//            final short prefixlen = bTreePageHeader.getPrefixLength();
            // Write out the Values
            Data prevKey = null;
            for (int i = 0; i < keys.length; i++) {
                final Data v = keys[i];
                //如果与前一个值相同，写-1，节省空间
                if (v == prevKey) {
                    os.writeInt(-1);
                } else {
                    final int len = v.getLength();
                    os.writeInt(len);
                    if (len > 0) {
                        v.writeTo(os, 0, len);
                    }
                }
                prevKey = v;
            }
            // Write out the pointers
            for (int i = 0; i < ptrs.length; i++) {
                VariableByteCodec.encodeUnsignedLong(ptrs[i], os);
            }
            // Write out link if current node is a leaf
            if (bTreePageHeader.getStatus() == LEAF) {
                os.writeLong(prev);
                os.writeLong(next);
            }

            writeData(page, new Data(bos.toByteArray()));
            this.parentCache = null;
            setDirty(false);
        }

        private int calculateDataLength() {
            if (currentDataLen > 0) {
                return currentDataLen;
            }
            final int vlen = keys.length;
            int datalen = (vlen >>> 2); */
/* key size *//*

            Data prevValue = null;
            for (int i = 0; i < vlen; i++) {
                final long ptr = ptrs[i];
                datalen += VariableByteCodec.requiredBytes(ptr);
                final Data v = keys[i];
                if (v == prevValue) {
                    continue;
                }
                final int actkeylen = v.getLength();
                datalen += actkeylen;
                prevValue = v;
            }
            if (bTreePageHeader.getStatus() == LEAF) {
                datalen += 16;
            }
            this.currentDataLen = datalen;
            return datalen;
        }

        private void incrDataLength(Data key, final long ptr) {
            int datalen = currentDataLen;
            if (datalen == -1) {
                datalen = calculateDataLength();
            }
            final int refcnt = key.incrRefCount();
            if (refcnt == 1) {
                datalen += key.getLength();
            }
            datalen += VariableByteCodec.requiredBytes(ptr);
            datalen += 4 */
/* key size *//*
;
            this.currentDataLen = datalen;
        }

        private void decrDataLength(final Data value) {
            int datalen = currentDataLen;
            final int refcnt = value.decrRefCount();
            if (refcnt == 0) {
                datalen -= value.getLength();
            }
            datalen -= (4 + 8);
            this.currentDataLen = datalen;
        }

        */
/** find lest-most value which matches to the key *//*

        long findValue( Data searchKey) throws IOException {
            int idx = BinarySearchUtils.searchLeftmostKey(keys, searchKey, keys.length);
            switch (bTreePageHeader.getStatus()) {
                case BRANCH:
                    idx = idx < 0 ? -(idx + 1) : idx + 1;
                    return getChildNode(idx).findValue(searchKey);
                case LEAF:
                    return ptrs[idx];
                   */
/* if (idx < 0) {
                        return KEY_NOT_FOUND;
                    } else {
                        if (idx == 0) {
                            BTreeNode leftmostNode = this;
                            //因为有重复 需要向前做查找相同的数据
                            while (true && leftmostNode.prev!=NO_PAGE) {
                                leftmostNode = getBTreeNode(root, leftmostNode.prev);
                                final Data[] lmKeys = leftmostNode.keys;
                                assert (lmKeys.length > 0);
                                if (!lmKeys[0].equals(searchKey)) {
                                    break;
                                }
                            }
                            final Data[] lmKeys = leftmostNode.keys;
                            final int lmIdx = BinarySearchUtils.searchLeftmostKey(lmKeys, searchKey,
                                lmKeys.length);
                            if (lmIdx < 0) {
                                throw new BTreeCorruptException(
                                    "Duplicated key was not found: " + searchKey);
                            }
                            final long[] leftmostPtrs = leftmostNode.ptrs;
                            return leftmostPtrs[lmIdx];
                        } else {
                            return ptrs[idx];
                        }
                    }*//*

                default:
                    throw new BTreeCorruptException(
                        "Invalid page type '" + bTreePageHeader.getStatus() + "' in findValue");
            }
        }

        BTreeNode getLeftLeafNode(final Data key) throws IOException {
            final byte nodeType = bTreePageHeader.getStatus();
            switch (nodeType) {
                case BRANCH:
                    int leftIdx = BinarySearchUtils.searchLeftmostKey(keys, key, keys.length);
                    leftIdx = leftIdx < 0 ? -(leftIdx + 1) : leftIdx + 1;
                    return getChildNode(leftIdx).getLeftLeafNode(key);
                case LEAF:
                    if (keys.length == 0) {
                        break;
                    }
                    BTreeNode leftmostNode = this;
                    //如果没有查找到key，返回后一个 如：this{1 2 3}，{5 6 7}，查找4，应返回this.next
                    if(keys[keys.length-1].compareTo(key)<0){
                        leftmostNode = getBTreeNode(root, leftmostNode.next);
//                        return leftmostNode;
                    }else if (keys[0].equals(key) && leftmostNode.prev!=NO_PAGE) {
                        while (true) {
                            leftmostNode = getBTreeNode(root, leftmostNode.prev);
                            Data[] dKeys = leftmostNode.keys;
                            Data firstKey = dKeys[0];
                            Data lastKey = dKeys[dKeys.length-1];
                            if (!firstKey.equals(key)) {
                                if(lastKey.compareTo(key)<0){
                                    leftmostNode = getBTreeNode(root, leftmostNode.next);
                                }
                                break;
                            }
                        }
                    }
                    return leftmostNode;
                    default:
                        return null;
            }
            return this;
        }

        BTreeNode getRightLeafNode(final Data key) throws IOException {
            final byte nodeType = bTreePageHeader.getStatus();
            switch (nodeType) {
                case BRANCH:
                    int leftIdx = BinarySearchUtils.searchRightmostKey(keys, key, keys.length);
                    leftIdx = leftIdx < 0 ? -(leftIdx + 1) : leftIdx + 1;
                    return getChildNode(leftIdx).getRightLeafNode(key);
                case LEAF:
                    if (keys.length == 0) {
                        break;
                    }
                    BTreeNode rightmostNode = this;
                    if (keys[keys.length-1].equals(key)) {
                        while (true && rightmostNode.next!=NO_PAGE) {
                            rightmostNode = getBTreeNode(root, rightmostNode.next);
                            int keylen = rightmostNode.keys.length;
                            Data firstKey = rightmostNode.keys[0];
                            Data lastKey = rightmostNode.keys[keylen-1];
                            if (!lastKey.equals(key)) {
                                if(firstKey.compareTo(key)>0){
                                    rightmostNode = getBTreeNode(root, rightmostNode.prev);
                                }
                                break;
                            }
                        }
                    }
                    return rightmostNode;
                default:
                    return null;
            }
            return this;
        }

        */
/**
         * Scan the leaf node. Note that keys might be shortest-possible value.
         * edge=0 最后一页
         *//*

        List<Data> scanLeaf(Data min, Data max, int edge) {
            List<Data> dataList = new ArrayList<>();
            assert (bTreePageHeader.getStatus() == LEAF) : bTreePageHeader.getStatus();
            int leftIdx=0;
            int rightIdx=0;
            if(edge==0){
                rightIdx = BinarySearchUtils.searchRightmostKey(keys, max, keys.length);
                if (rightIdx < 0) {
                    //未找到key 则找到与key最接近的值 需要2-right
                    rightIdx = -(rightIdx + 2);
                }
            }else {
                rightIdx = keys.length;
                if (keys[0].compareTo(min) < 0) {
                    leftIdx = BinarySearchUtils.searchLeftmostKey(keys, min, keys.length);
                    if (leftIdx < 0) {
                        leftIdx = -(leftIdx + 1);
                    }
                }
            }
            */
/*int leftIdx = BinarySearchUtils.searchLeftmostKey(keys, min, keys.length);
            if (leftIdx < 0) {
                leftIdx = -(leftIdx + 1);
            }*//*

           */
/* int rightIdx = BinarySearchUtils.searchRightmostKey(keys, max, keys.length);
            if (rightIdx < 0) {
                rightIdx = -(rightIdx + 1);
            }*//*

            for (int i = leftIdx; i < ptrs.length; i++) {
                if (i <= rightIdx) {
                    dataList.add(keys[i]);
//                            callback.indexInfo(keys[i], ptrs[i]);
                }
            }
            return dataList;
        }

        */
/**
         * 不能随便用 在LRUCache中使用会出错
         * @return
         *//*

        @Override
        public String toString() {
            final StringBuilder buf = new StringBuilder();
            final long rootPage = root.getPage();
            BTreeNode pn = this;
            while (true) {
                final long curPageNum = pn.page.getPageNum();
                buf.append(curPageNum);
                pn = pn.getParent();
                if (pn == null) {
                    if (curPageNum != rootPage) {
                        buf.append("<-?");
                    }
                    break;
                } else {
                    buf.append("<-");
                }
            }
            return buf.toString();
        }

        public Page getPage() {
            return page;
        }

        @Override
        public int compareTo(BTreeNode other) {
            return page.compareTo(other.page);
        }
    }*/
