package com.huazi.io.iotest.btree;

import com.huazi.io.iotest.Constant;
import com.huazi.io.iotest.btree.header.FileHeader;
import com.huazi.io.iotest.btree.page.BtreePage;
import com.huazi.io.iotest.cache.LRUCache;
import com.huazi.io.iotest.util.ArrayUtils;
import com.huazi.io.iotest.util.BinarySearchUtils;
import com.huazi.io.iotest.util.Primitives;

import java.io.IOException;
import java.util.Arrays;

import static com.huazi.io.iotest.Constant.BRANCH;
import static com.huazi.io.iotest.Constant.LEAF;

public class Btree {
    private BTreeRootInfo rootInfo;
    private BTreeNode rootNode;
    private FileHeader fileHeader;
    private BTreeNode parentCache;

//    private LRUCache<Long,BtreePage> lruCache = new LRUCache<>(10);
    private LRUCache<Long,BTreeNode> nodeLruCache = new LRUCache<>(10);

    public Btree(FileHeader fileHeader) {
        this.fileHeader = fileHeader;
    }

    public void init() throws IOException {
        if(fileHeader.getHasRoot()==1){
            long rootPageNum = fileHeader.getRootPage();
            rootInfo = new BTreeRootInfo(rootPageNum);
            rootNode = new BTreeNode(rootInfo,getPage(rootPageNum));
            rootNode.read();
            System.out.println(rootNode.keys);
        }else {
            rootInfo = new BTreeRootInfo(1);
            rootNode = new BTreeNode(rootInfo,getFreePage());
            rootNode.keys = new long[0];
            rootNode.ptrs = new long[0];
            rootNode.btreeRootInfo = rootInfo;
        }

    }

    public BTreeNode getRootNode() {
        return rootNode;
    }

    public LRUCache<Long, BTreeNode> getNodeLruCache() {
        return nodeLruCache;
    }

    public void setNodeLruCache(LRUCache<Long, BTreeNode> nodeLruCache) {
        this.nodeLruCache = nodeLruCache;
    }

    private BtreePage getFreePage() {
        BtreePage btreePage =  new BtreePage(fileHeader,fileHeader.incrTotalPage());
//        lruCache.put(btreePage.getPageNum(),btreePage);
        return btreePage;
    }

    private final BTreeNode getBTreeNode(BTreeRootInfo root, long page, BTreeNode parent) throws IOException {

        BTreeNode node = nodeLruCache.get(page);
        if(node!=null){
            return node;
        }
        if(page==rootInfo.getPage()){
            return rootNode;
        }
        node = new BTreeNode(root, getPage(page), parent);
        try {
            node.read();
        } catch (IOException e) {
            throw new IOException("failed to read page#" + page, e);
        }
        nodeLruCache.put(page,node);
        return node;
    }

    protected final BtreePage getPage(long pageNum) throws IOException {
//        BtreePage page =lruCache.get(pageNum);
        BtreePage page = null;
        if(page==null) {
            page = new BtreePage(fileHeader, pageNum);
            //从磁盘中加载页数据
            page.readPage();
//            lruCache.put(pageNum,page);
        }
        return page;
    }
    
    public void add(long key,long ptr) throws IOException {
        rootNode.addValue(key,ptr);
    }

    public long find(long key) throws IOException {
        return rootNode.find(key);
    }

    public final class BTreeNode implements Comparable<BTreeNode> {
        private static final int LEAST_KEYS = 200;

        private final BTreeRootInfo root;
        private final BtreePage page;

        private BTreeNode parentCache;

        private long[] keys;
        private long[] ptrs;
        private long next = -1;
        private long prev = -1;

        private boolean loaded = false;
        private int currentDataLen = -1;
        private boolean dirty = false;
        private BTreeRootInfo btreeRootInfo;
        private BTreeNode btreeRootNode;


        protected BTreeNode(final BTreeRootInfo root, final BtreePage page, final BTreeNode parentNode) {
            this.root = root;
            this.page = page;
//            page.setParent(parentNode);
//            long parentNum = parentNode==null ? -1 : parentNode.getPage().getPageNum();
//            bTreePageHeader.setParentPageNum(parentNum);
            this.setParent(parentNode);
            this.parentCache = parentNode;
        }

        protected BTreeNode(final BTreeRootInfo root, final BtreePage page) {
            this.root = root;
            this.page = page;
        }

        private void setParent(BTreeNode node) {
            long parentPage = node.page.getPageNum();
            if (parentPage != page.getParentPage()) {
                page.setParentPage(parentPage);
                this.parentCache = node;
                this.dirty = true;
            }
        }

        private BTreeNode getParent() {
            if (parentCache != null) {
                return parentCache;
            }
            long pageNum = this.page.getParentPage();
            if (pageNum != Constant.NO_PAGE) {
                try {
                    parentCache = getBTreeNode(btreeRootInfo, page.getPageNum(), null);
                    //todo 从磁盘重新初始化的root要赋给btreeRootNode
                    if (parentCache.page.getPageNum() == btreeRootNode.page.getPageNum()) {
                        btreeRootNode = parentCache;
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("failed to get parent page #" + page, e);
                }
                return parentCache;
            }
            return null;
        }


        long addValue(long key, final long pointer) throws IOException {
            int idx = BinarySearchUtils.searchRightmostKey(keys, key, keys.length);
            switch (page.getType()) {
                case BRANCH: {
                    idx = idx < 0 ? -(idx + 1) : idx + 1;
                    return getChildNode(idx).addValue(key, pointer);
                }
                case LEAF: {
                    final boolean found = idx >= 0;
                    final long oldPtr;
                    if (found) {
                        oldPtr = ptrs[idx];
                        key = keys[idx]; // use the existing key object
                        idx = idx + 1;
                    } else {
                        oldPtr = -1;
                        idx = -(idx + 1);
                    }
                    set(ArrayUtils.insert(keys, idx, key),
                            ArrayUtils.insert(ptrs, idx, pointer));
//                    incrDataLength(key, pointer);

                    // Check to see if we've exhausted the block
                    if (needSplit()) {
                        split();
                    }
                    return oldPtr;
                }
                default:
                    throw new IOException("错误");
            }
        }


        private BTreeNode getChildNode(final int idx) throws IOException {
            if (page.getType() == BRANCH && idx >= 0 && idx < ptrs.length) {
                return getBTreeNode(root, ptrs[idx], this);
            }
            return null;
        }


        private boolean needSplit() {
            int afterKeysLength = keys.length;

            if (afterKeysLength < BtreePage.maxDataLen) {
                return false;
            }
            return true;
        }


        private void split() throws IOException {
            final long[] leftVals;
            final long[] rightVals;
            final long[] leftPtrs;
            final long[] rightPtrs;
            final long separator;

            final short vc = (short) this.keys.length;
            int pivot = vc - 1;

            // Split the node into two nodes
            final byte pageType = page.getType();
            switch (pageType) {
                case BRANCH: {
                    leftVals = new long[pivot];
                    leftPtrs = new long[leftVals.length + 1];
                    rightVals = new long[vc - (pivot + 1)];
                    rightPtrs = new long[rightVals.length + 1];

                    System.arraycopy(keys, 0, leftVals, 0, leftVals.length);
                    System.arraycopy(ptrs, 0, leftPtrs, 0, leftPtrs.length);
                    System.arraycopy(keys, leftVals.length + 1, rightVals, 0, rightVals.length);
                    System.arraycopy(ptrs, leftPtrs.length, rightPtrs, 0, rightPtrs.length);

                    separator = keys[leftVals.length];
                    break;
                }
                case LEAF: {
                    leftVals = new long[pivot];
                    leftPtrs = new long[leftVals.length];
                    rightVals = new long[vc - pivot];
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
                    throw new IOException("Invalid page type in split: " + pageType);
            }

            // Promote the pivot to the parent branch
            final BTreeNode parent = getParent(); // this node may be GC'd
            if (parent == null) {
                // This can only happen if this is the root
                BTreeNode lNode = createBTreeNode(root, pageType, this);
                lNode.set(leftVals, leftPtrs);
//                lNode.calculateDataLength();
                lNode.setAsParent();

                BTreeNode rNode = createBTreeNode(root, pageType, this);
                rNode.set(rightVals, rightPtrs);
//                rNode.calculateDataLength();
                rNode.setAsParent();

                if (pageType == LEAF) {
                    setLeavesLinked(lNode, rNode);
                }

                page.setType(BRANCH);
                set(new long[]{separator},
                        new long[]{lNode.page.getPageNum(), rNode.page.getPageNum()});
//                calculateDataLength();
            } else {
                set(leftVals, leftPtrs);
//                calculateDataLength();

                BTreeNode rNode = createBTreeNode(root, pageType, parent);
                rNode.set(rightVals, rightPtrs);
//                rNode.calculateDataLength();
                rNode.setAsParent();

                if (pageType == LEAF) {
                    setLeavesLinked(this, rNode);
                }

                long leftPtr = page.getPageNum();
                long rightPtr = rNode.page.getPageNum();
                parent.promoteValue(separator, leftPtr, rightPtr);
            }
        }


        private final BTreeNode createBTreeNode(BTreeRootInfo root, byte type, BTreeNode parent) throws IOException {
            if (parent == null) {
                throw new IllegalArgumentException();
            }
            BtreePage p = getFreePage();
            BTreeNode node = new BTreeNode(root, p, parent);
            //node.set(new Value[0], new long[0]);
            node.page.setType(type);
            nodeLruCache.put(node.page.getPageNum(),node);
            return node;
        }

        private void setAsParent() throws IOException {
            if (page.getType() == BRANCH) {
                for (final long ptr : ptrs) {
                    BTreeNode child = getBTreeNode(btreeRootInfo, ptr, this);
                    child.setParent(this);
                }
            }
        }


        private void setLeavesLinked(final BTreeNode left, final BTreeNode right) throws IOException {
            final long leftPageNum = left.page.getPageNum();
            final long rightPageNum = right.page.getPageNum();
            final long origNext = left.next;
            if (origNext != -1L) {
                right.next = origNext;
                BTreeNode origNextNode = getBTreeNode(root, origNext, null);
                origNextNode.prev = rightPageNum;
                origNextNode.setDirty(true);
            }
            left.next = rightPageNum;
            right.prev = leftPageNum;
        }

        private void promoteValue(final long key, final long leftPtr, final long rightPtr) throws IOException {
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
            set(ArrayUtils.insert(keys, insertPoint, key),
                    ArrayUtils.insert(ptrs, insertPoint + 1, rightPtr));
//            incrDataLength(key, rightPtr);
            // Check to see if we've exhausted the block
            if (needSplit()) {
                split();
            }
        }


        //todo String类型为啥可以
        private long getSeparator(final long value1, final long value2) {
            return value2;
        }


        private void set(final long[] values, final long[] ptrs) {
            final int vlen = values.length;
            if (vlen > Short.MAX_VALUE) {
                throw new IllegalArgumentException("entries exceeds limit: " + vlen);
            }
            this.keys = values;
            this.ptrs = ptrs;
            setDirty(true);
        }

        private void setDirty(final boolean dirt) {
            this.dirty = dirt;
        }


        public void read() throws IOException {
            if (!this.loaded) {
                page.readPage();
                keys = page.getKeys();
                ptrs = page.getPtrs();
                // Read in the links if current node is a leaf
                if (page.getType() == LEAF) {
                    this.prev = page.getNextPage();
                    this.next = page.getNextPage();
                }
                this.currentDataLen = keys.length;
                this.loaded = true;
            }
        }

        public void write() throws IOException {
            if (!dirty) {
                return;
            }
            /*System.out.println((page.getType() == LEAF ? "Leaf " : "Branch ") + "Node#"
                    + page.getPageNum() + " - " + Arrays.toString(keys));*/
            int dataCount = keys.length + ptrs.length;
            long[] dataLong = new long[dataCount];
            System.arraycopy(keys, 0, dataLong, 0, keys.length);
            System.arraycopy(ptrs, 0, dataLong, keys.length, ptrs.length);
            byte[] dataBytes = Primitives.toBytes(dataLong);

            page.setBuffer(dataBytes);
            // Write out link if current node is a leaf
            if (page.getType() == LEAF) {
                page.setParentPage(prev);
                page.setNextPage(next);
            }
            page.write();
            this.parentCache = null;
            setDirty(false);
        }

//        private int calculateDataLength() {
//            if (currentDataLen > 0) {
//                return currentDataLen;
//            }
//            final int vlen = keys.length;
//            int datalen = (vlen >>> 2);
//            key size
//
//            Data prevValue = null;
//            for (int i = 0; i < vlen; i++) {
//                final long ptr = ptrs[i];
//                datalen += VariableByteCodec.requiredBytes(ptr);
//                final Data v = keys[i];
//                if (v == prevValue) {
//                    continue;
//                }
//                final int actkeylen = v.getLength();
//                datalen += actkeylen;
//                prevValue = v;
//            }
//            if (bTreePageHeader.getStatus() == LEAF) {
//                datalen += 16;
//            }
//            this.currentDataLen = datalen;
//            return datalen;
//        }
//
//        private void decrDataLength(final Data value) {
//            int datalen = currentDataLen;
//            final int refcnt = value.decrRefCount();
//            if (refcnt == 0) {
//                datalen -= value.getLength();
//            }
//            datalen -= (4 + 8);
//            this.currentDataLen = datalen;
//        }
//
//        long findValue( Data searchKey) throws IOException {
//            int idx = BinarySearchUtils.searchLeftmostKey(keys, searchKey, keys.length);
//            switch (bTreePageHeader.getStatus()) {
//                case BRANCH:
//                    idx = idx < 0 ? -(idx + 1) : idx + 1;
//                    return getChildNode(idx).findValue(searchKey);
//                case LEAF:
//                    return ptrs[idx];
//             if (idx < 0) {
//                        return KEY_NOT_FOUND;
//                    } else {
//                        if (idx == 0) {
//                            BTreeNode leftmostNode = this;
//                            //因为有重复 需要向前做查找相同的数据
//                            while (true && leftmostNode.prev!=NO_PAGE) {
//                                leftmostNode = getBTreeNode(root, leftmostNode.prev);
//                                final Data[] lmKeys = leftmostNode.keys;
//                                assert (lmKeys.length > 0);
//                                if (!lmKeys[0].equals(searchKey)) {
//                                    break;
//                                }
//                            }
//                            final Data[] lmKeys = leftmostNode.keys;
//                            final int lmIdx = BinarySearchUtils.searchLeftmostKey(lmKeys, searchKey,
//                                lmKeys.length);
//                            if (lmIdx < 0) {
//                                throw new BTreeCorruptException(
//                                    "Duplicated key was not found: " + searchKey);
//                            }
//                            final long[] leftmostPtrs = leftmostNode.ptrs;
//                            return leftmostPtrs[lmIdx];
//                        } else {
//                            return ptrs[idx];
//                        }
//                    }
//
//                default:
//                    throw new BTreeCorruptException(
//                        "Invalid page type '" + bTreePageHeader.getStatus() + "' in findValue");
//            }
//        }

//        BTreeNode getLeftLeafNode(final Data key) throws IOException {
//            final byte nodeType = bTreePageHeader.getStatus();
//            switch (nodeType) {
//                case BRANCH:
//                    int leftIdx = BinarySearchUtils.searchLeftmostKey(keys, key, keys.length);
//                    leftIdx = leftIdx < 0 ? -(leftIdx + 1) : leftIdx + 1;
//                    return getChildNode(leftIdx).getLeftLeafNode(key);
//                case LEAF:
//                    if (keys.length == 0) {
//                        break;
//                    }
//                    BTreeNode leftmostNode = this;
//                    //如果没有查找到key，返回后一个 如：this{1 2 3}，{5 6 7}，查找4，应返回this.next
//                    if(keys[keys.length-1].compareTo(key)<0){
//                        leftmostNode = getBTreeNode(root, leftmostNode.next);
////                        return leftmostNode;
//                    }else if (keys[0].equals(key) && leftmostNode.prev!=NO_PAGE) {
//                        while (true) {
//                            leftmostNode = getBTreeNode(root, leftmostNode.prev);
//                            Data[] dKeys = leftmostNode.keys;
//                            Data firstKey = dKeys[0];
//                            Data lastKey = dKeys[dKeys.length-1];
//                            if (!firstKey.equals(key)) {
//                                if(lastKey.compareTo(key)<0){
//                                    leftmostNode = getBTreeNode(root, leftmostNode.next);
//                                }
//                                break;
//                            }
//                        }
//                    }
//                    return leftmostNode;
//                    default:
//                        return null;
//            }
//            return this;
//        }
//
//        BTreeNode getRightLeafNode(final Data key) throws IOException {
//            final byte nodeType = bTreePageHeader.getStatus();
//            switch (nodeType) {
//                case BRANCH:
//                    int leftIdx = BinarySearchUtils.searchRightmostKey(keys, key, keys.length);
//                    leftIdx = leftIdx < 0 ? -(leftIdx + 1) : leftIdx + 1;
//                    return getChildNode(leftIdx).getRightLeafNode(key);
//                case LEAF:
//                    if (keys.length == 0) {
//                        break;
//                    }
//                    BTreeNode rightmostNode = this;
//                    if (keys[keys.length-1].equals(key)) {
//                        while (true && rightmostNode.next!=NO_PAGE) {
//                            rightmostNode = getBTreeNode(root, rightmostNode.next);
//                            int keylen = rightmostNode.keys.length;
//                            Data firstKey = rightmostNode.keys[0];
//                            Data lastKey = rightmostNode.keys[keylen-1];
//                            if (!lastKey.equals(key)) {
//                                if(firstKey.compareTo(key)>0){
//                                    rightmostNode = getBTreeNode(root, rightmostNode.prev);
//                                }
//                                break;
//                            }
//                        }
//                    }
//                    return rightmostNode;
//                default:
//                    return null;
//            }
//            return this;
//        }
//
//        List<Data> scanLeaf(Data min, Data max, int edge) {
//            List<Data> dataList = new ArrayList<>();
//            assert (bTreePageHeader.getStatus() == LEAF) : bTreePageHeader.getStatus();
//            int leftIdx=0;
//            int rightIdx=0;
//            if(edge==0){
//                rightIdx = BinarySearchUtils.searchRightmostKey(keys, max, keys.length);
//                if (rightIdx < 0) {
//                    //未找到key 则找到与key最接近的值 需要2-right
//                    rightIdx = -(rightIdx + 2);
//                }
//            }else {
//                rightIdx = keys.length;
//                if (keys[0].compareTo(min) < 0) {
//                    leftIdx = BinarySearchUtils.searchLeftmostKey(keys, min, keys.length);
//                    if (leftIdx < 0) {
//                        leftIdx = -(leftIdx + 1);
//                    }
//                }
//            }
//int leftIdx = BinarySearchUtils.searchLeftmostKey(keys, min, keys.length);
//            if (leftIdx < 0) {
//                leftIdx = -(leftIdx + 1);
//            }
//
// int rightIdx = BinarySearchUtils.searchRightmostKey(keys, max, keys.length);
//            if (rightIdx < 0) {
//                rightIdx = -(rightIdx + 1);
//            }
//
//            for (int i = leftIdx; i < ptrs.length; i++) {
//                if (i <= rightIdx) {
//                    dataList.add(keys[i]);
////                            callback.indexInfo(keys[i], ptrs[i]);
//                }
//            }
//            return dataList;
//        }

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

        public BtreePage getPage() {
            return page;
        }

        @Override
        public int compareTo(BTreeNode other) {
            return page.compareTo(other.page);
        }

        public long find(long key) throws IOException {
            int idx = BinarySearchUtils.searchLeftmostKey(keys,key , keys.length);
            switch (page.getType()) {
                case BRANCH:
                    idx = idx < 0 ? -(idx + 1) : idx + 1;
                    return getChildNode(idx).find(key);
                case LEAF:
                    return ptrs[idx];
                default:
                    throw new IOException("Invalid page type in findValue");
            }
        }
    }
}
