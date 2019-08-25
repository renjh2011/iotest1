package com.huazi.io.iotest.util;



public class BinarySearchUtils {
    public static int searchRightmostKey(final long[] ary, final long key, final int to) {
        int low = 0;
        int high = to - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = ary[mid];
            long cmp = midVal-key;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                for (int i = mid + 1; i <= high; i++) {
                    long nxtVal = ary[i];
                    cmp = midVal-nxtVal;
                    if (cmp != 0) {
                        break;
                    }
                    mid = i;
                }
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }

    public static int searchLeftmostKey(final long[] ary, final long key, final int to) {
        int low = 0;
        int high = to - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = ary[mid];
            long cmp = midVal-key;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                for (int i = mid - 1; i >= 0; i--) {
                    long nxtVal = ary[i];
                    cmp = midVal-nxtVal;
                    if (cmp != 0) {
                        break;
                    }
                    mid = i;
                }
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }
}
