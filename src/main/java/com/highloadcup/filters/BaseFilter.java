package com.highloadcup.filters;

import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

import static com.highloadcup.DirectMemoryAllocator.BAD_ACCOUNT_IDX;

public abstract class BaseFilter {

    private static final Logger logger = LogManager.getLogger(BaseFilter.class);

    public final Object lock = new Object();

    protected String predicate;
    protected String value;
    public int limit;
    public int calculatedPriority = Integer.MAX_VALUE;

    public boolean isSingle = false;
    public boolean isLast = false;

    public BaseFilter() {
    }

    public BaseFilter(String predicate, String[] value, int limit) {
        this.predicate = predicate;
        this.value = value == null ? null : value[0].trim();
        this.limit = limit;
    }

    public void fill(String predicate, String[] value, int limit) {
        this.predicate = predicate;
        this.value = value == null ? null : value[0].trim();
        this.limit = limit;
    }

    public void init() {
        //logger.error("{} init start", this.getClass().getName());
    }

    public void reset() {
        isSingle = false;
        isLast = false;
    }

    public abstract BaseFilter clone(String predicate, String[] value, int limit);

    public void calculatePriority() {}

    public int[] getFilteredSet(int[] currentSet) {
        return currentSet;
    }

    public int[] getFilteredSet(int[] currentSet, int limit) {
        return currentSet;
    }

    public long[] getBitmapFilteredSet(long[] currentSet) {
        return currentSet;
    }

    public void fillBitSetForGroup(long[] bitSet) {

    }

    public void intersect(long[] bitSet) {

    }

    public synchronized void processNew(InternalAccount a) {

    }

    public synchronized void processUpdate(InternalAccount a) {

    }

    public boolean checkAccount(int account) {
        return true;
    }

    public abstract boolean validatePredicateAndVal(String predicat, String[] value);

    public static void invalidateSetNonEquals(int[] currentSet, int[] valuesArray, int compareIndex) {
        for (int i = 0; i < currentSet.length; i++) {
            if (currentSet[i] != BAD_ACCOUNT_IDX && valuesArray[currentSet[i]] != compareIndex) {
                currentSet[i] = BAD_ACCOUNT_IDX;
            }
        }
    }

    public static void invalidateSetEquals(int[] currentSet, int[] valuesArray, int compareIndex) {
        for (int i = 0; i < currentSet.length; i++) {
            if (currentSet[i] != BAD_ACCOUNT_IDX && valuesArray[currentSet[i]] == compareIndex) {
                currentSet[i] = BAD_ACCOUNT_IDX;
            }
        }
    }

    public static void invalidateSetNonEquals(int[] currentSet, long[] valuesArray, int compareIndex) {
        for (int i = 0; i < currentSet.length; i++) {
            if (currentSet[i] != BAD_ACCOUNT_IDX && valuesArray[currentSet[i]] != compareIndex) {
                currentSet[i] = BAD_ACCOUNT_IDX;
            }
        }
    }

    public static void invalidateSetEquals(int[] currentSet, long[] valuesArray, int compareIndex) {
        for (int i = 0; i < currentSet.length; i++) {
            if (currentSet[i] != BAD_ACCOUNT_IDX && valuesArray[currentSet[i]] == compareIndex) {
                currentSet[i] = BAD_ACCOUNT_IDX;
            }
        }
    }

    public static class SortedNode {
        public SortedNode(int value, int accountIdx) {
            this.value = value;
            this.accountIdx = accountIdx;
        }
        public int value;
        public int accountIdx;
    }

    public static class SortedLongNode {
        public SortedLongNode(long value, int accountIdx) {
            this.value = value;
            this.accountIdx = accountIdx;
        }
        public long value;
        public int accountIdx;
    }

    public static int[] copySet(int[] current) {
        int length = current.length;
        int[] newSet = new int[length];
        System.arraycopy(current, 0, newSet, 0, length);
        return newSet;
    }

    public static long[] copyBitMap(long[] current) {
        int length = current.length;
        long[] newSet = new long[length];
        System.arraycopy(current, 0, newSet, 0, length);
        return newSet;
    }

    public static int[] realloc(int[] curr, int newSize) {
        int[] newArr = new int[newSize];
        System.arraycopy(curr, 0, newArr, 0,
                curr.length > newSize ? newSize : curr.length);
        return newArr;
    }

    public static short[] realloc(short[] curr, int newSize) {
        short[] newArr = new short[newSize];
        System.arraycopy(curr, 0, newArr, 0,
                curr.length > newSize ? newSize : curr.length);
        return newArr;
    }

    //TODO
    public static int[] returnIntersect(int[] set1, int[] set2) {
        if (set1 == null && set2 == null) {
            return null;
        } else if ((set1 != null && set1.length == 0) || (set2 != null && set2.length == 0)) {
            return new int[0];
        } else if (set1 != null && set2 != null) {
            return Arrays.stream(set1).filter(x -> Arrays.stream(set2).anyMatch(y -> y == x)).toArray();
        } else if (set1 == null) {
            return set2;
        } else {
            return set1;
        }
    }

    public static int findPositionBinarySearch(int arr[], int l, int r, long idx)
    {
        if ((arr.length == r - l + 1)) {
            if (idx < arr[l]) {
                return l - 1;
            } else if(idx > arr[r]) {
                return r + 1;
            }
        }
        if (r > l)
        {
            int mid = l + (r - l)/2;
            if (idx == arr[mid])
                return mid;
            if (idx < arr[mid]) {
                return findPositionBinarySearch(arr, l, mid - 1, idx);
            } else {
                return findPositionBinarySearch(arr, mid + 1, r, idx);
            }
        } else if (r <= l) {
            return r;
        }
        return -1;
    }

    public static int findPositionBinarySearch(long arr[], int l, int r, long idx)
    {
        if ((arr.length == r - l + 1)) {
            if (idx < arr[l]) {
                return l - 1;
            } else if(idx > arr[r]) {
                return r + 1;
            }
        }
        if (r > l)
        {
            int mid = l + (r - l)/2;
            if (idx == arr[mid])
                return mid;
            if (idx < arr[mid]) {
                return findPositionBinarySearch(arr, l, mid - 1, idx);
            } else {
                return findPositionBinarySearch(arr, mid + 1, r, idx);
            }
        } else if (r <= l) {
            return r;
        }
        return -1;
    }

}
