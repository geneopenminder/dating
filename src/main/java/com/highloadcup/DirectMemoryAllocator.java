package com.highloadcup;

import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.*;

public class DirectMemoryAllocator {

    public static int BAD_ACCOUNT_IDX = 0xFFFFFF;

    //static ThreadLocal<long[]> stringBuilderThreadLocal = ThreadLocal.withInitial(() -> long[numAccounts/64 + (numAccounts%64 > 0 ? 1 : 0)]);
    static final long[] ffArray = new long[MAX_ACCOUNTS / 64 + 1];
    static final int[] ffArrayInt = new int[MAX_ACCOUNTS / 64 + 1];
    static final long[] zeroLongArray = new long[MAX_ACCOUNTS / 64 + 1];
    static final int[] zeroIntArray = new int[MAX_ACCOUNTS + 1000];
    static final byte[] zeroByteArray = new byte[500000];

    static final char[] zeroCharArray = new char[50000];

    /*
        fullScanAccounts structure

        long emailIdx;
        int birthDate;
        int reserved;
    */

    public static int emailIdxOffsetInt = 0;
    public static int birthDateOffsetInt = 2;

    public static int[] fullScanAccounts = new int[MAX_ACCOUNTS*4];

    static {
        Arrays.fill(ffArray, 0xffffffffffffffffL);
        Arrays.fill(ffArrayInt, 0xffffffff);

        for (int i = 0; i < zeroCharArray.length; i++) {
            zeroCharArray[i] = ' ';
        }
    }

    public static void putBirthDateToFS(int accId, int birthDate) {
        int offset = accId * 4 + birthDateOffsetInt;
        fullScanAccounts[offset] = birthDate;
    }

    public static int getBirthDateFromFS(int accId) {
        int offset = accId * 4 + birthDateOffsetInt;
        return fullScanAccounts[offset];
    }

    public static void putEmailIdxToFS(int accId, long emailIdx) {
        int offset = accId * 4 + emailIdxOffsetInt;
        fullScanAccounts[offset] = (int) (emailIdx & (long)0xFFFFFFFF);
        fullScanAccounts[offset + 1] = (int) ((long)(emailIdx >>> 32) & (long)0xFFFFFFFFL);

    }

    public static long getEmailIdxFromFS(int accId) {
        int offset = accId * 4 + emailIdxOffsetInt;
        long result = fullScanAccounts[offset];
        result += (long)(Integer.toUnsignedLong(fullScanAccounts[offset + 1]) << 32);
        return result;
    }

    public static void clear(long[] set) {
        System.arraycopy(zeroLongArray, 0, set, 0, set.length);
    }

    public static void clear(char[] set) {
        System.arraycopy(zeroCharArray, 0, set, 0, set.length);
    }

    public static void clearFF(long[] set) {
        System.arraycopy(ffArray, 0, set, 0, set.length);
    }

    public static void clearFF(int[] set) {
        System.arraycopy(ffArrayInt, 0, set, 0, set.length);
    }

    public static void clear(int[] set) {
        System.arraycopy(zeroIntArray, 0, set, 0, set.length);
    }

    public static void clear(byte[] set, int length) {
        System.arraycopy(zeroByteArray, 0, set, 0, length);
    }

    //int -> 3bytes
    public static byte[] allocTriple(int numAccounts) {
        int byteCount = numAccounts*3;
        return new byte[byteCount];
    }

    public static class TripleIntArray {
        public byte[] data;
        public int position;

        public int size() {
            return data.length / 3;
        }
    }

    public static TripleIntArray allocTripleIntArr(int numAccounts) {
        int byteCount = numAccounts*3;
        TripleIntArray a = new TripleIntArray();
        a.data = new byte[byteCount];
        a.position = 0;
        return a;
    }

    public static int[] realloc(int[] old, int newSize) {
        int[] newA = new int[newSize];
        System.arraycopy(old, 0, newA, 0,
                old.length > newSize ? newSize : old.length);
        return newA;
    }

    public static void reallocTripleArr(TripleIntArray a, int newACount) {
        int byteCount = newACount*3;
        byte[] data = new byte[byteCount];
        System.arraycopy(a.data, 0, data, 0,
                a.data.length > byteCount ? byteCount : a.data.length);
        a.data = data;
    }

    public static TripleIntArray allocTripleIntArrAndFill(int numAccounts, int[] idx) {
        int byteCount = numAccounts*3;
        TripleIntArray a = new TripleIntArray();
        a.data = new byte[byteCount];
        a.position = numAccounts;

        for (int i = 0; i < idx.length; i++) {
            putToTriple(a.data, i, idx[i]);
        }
        return a;
    }

    public static int getFromTriple(byte[] array, int index) {
        int position = index * 3;
        int first = Byte.toUnsignedInt(array[position]);
        int second = Byte.toUnsignedInt(array[position + 1]) << (int)8;
        int third = Byte.toUnsignedInt(array[position + 2]) << (int)16;

        int result = first | second | third;
        return result;
    }

    public static void putToTriple(TripleIntArray a, int index, int val) {
        putToTriple(a.data, index, val);
    }

    public static void putToTriple(byte[] array, int index, int val) {
        int position = index * 3;
        byte first = (byte)((int)val & (int)0x000000FF);
        int second = (int)val & (int)0x0000FF00; // >>> (int)8);
        int third = (int)val & (int)0x00FF0000; // >>> (int)16);
        array[position] = first;
        array[position + 1] = (byte)(second >> 8);
        array[position + 2] = (byte)(third >> 16);
    }

    public static byte[] realloc(byte[] old, int newSize) {
        byte[] newA = new byte[newSize];
        System.arraycopy(old, 0, newA, 0,
                old.length > newSize ? newSize : old.length);
        return newA;
    }

    public static long[] alloc(int numAccounts) {
        return new long[numAccounts/64 + (numAccounts%64 > 0 ? 1 : 0)];
    }

    public static long[] allocWithFillFF(int numAccounts) {
        long[] set = new long[numAccounts/64 + (numAccounts%64 > 0 ? 1 : 0)];
        System.arraycopy(ffArray, 0, set, 0, set.length);
        return set;
    }

    public static long[] allocWithFill(int numAccounts, long value) {
        long[] set = new long[numAccounts/64 + (numAccounts%64 > 0 ? 1 : 0)];
        Arrays.fill(set, value);
        return set;
    }

    public static void unsetBit(long[] data, int index) {
        final int dataIndex = index >>> 6; // x/64
        long chunk = data[dataIndex];
        long bitIndex = ((long)index&0x0000003FL);
        long mask = ~(long)(0x01L << bitIndex);
        chunk = (chunk & mask);
        data[dataIndex] =  chunk;
    }

    public static void setBit(long[] data, int index) {
        final int dataIndex = index >>> 6; // x/64
        long chunk = data[dataIndex];
        long bitIndex = ((long)index&0x0000003FL);
        long mask = (long)(0x01L << bitIndex);
        chunk = (chunk | mask);
        data[dataIndex] =  chunk;
    }

    @Deprecated
    public static boolean setBitAnd(long[] data, int index) {
        final int dataIndex = index >>> 6; // x/5
        long chunk = data[dataIndex];
        long bitIndex = ((long)index&0x0000003FL);
        long mask = (long)(0x01L << bitIndex);
        long result = (chunk & mask);
        boolean isSet = result != 0;
        chunk = (chunk | result);
        data[dataIndex] =  chunk;
        return isSet;
    }

    public static boolean isBitSet(long[] data, int index) {
        final int dataIndex = index >>> 6; // x/5
        long chunk = data[dataIndex];
        if (chunk == 0) {
            return false;
        }
        long bitIndex = ((long)index&0x0000003FL);
        long mask = (long)(0x01L << bitIndex);
        return (chunk & mask) != 0;
    }


    public static boolean needSkipDesc(long[] data, int index) {
        //final long bitIndex = ((long) index & 0x0000003FL);
        //if (bitIndex != 0) {
        //    return 0;
        //}
        int dataIndex = index >>> 6; // x/5
        if (data[dataIndex] == 0) {
            //skip 63
            return true;
        }
        return false;
    }


    /*
    public static boolean needSkipDesc(long[] data, int index) {
        final int dataIndex = index >>> 6; // x/5
        if (dataIndex == 0) {
            return false;
        }
        long chunk = data[dataIndex - 1];
        if (chunk == 0) {
            return true;
        }
        return false;
    }
    */

    public static void intersectArrays(long[] a1, long[] a2) {
        for (int i = 0; i < a1.length; i++) {
            long val = a1[i];
            a1[i] = val & a2[i];
        }
    }

    public static boolean isDiff(long[] a1, long[] a2) {
        for (int i = 0; i < a1.length; i++) {
            if (a1[i] != a2[i]) {
                return true;
            }
        }
        return false;
    }

    public static void bisectArrays(long[] a1, long[] a2) {
        for (int i = 0; i < a1.length; i++) {
            long val = a1[i];
             a1[i] = val | a2[i];
        }
    }

    //optimal limit = 100; ~300 nanos
    public static void intersectArraysLimitDesc(long[] a1, long[] a2, int limit) {
        if (limit < 100) {
            limit = 100;
        }
        int count = 0;
        for (int i = MAX_ACC_ID_ADDED.get()/64 + 1; i >= 0; i--) {
            long val = a1[i];
            val = val&a2[i];
            a1[i] = val;
            count += Long.bitCount(val);
            if (count >= limit) {
                break;
            }
        }
    }

    //optimal limit = 100; ~300 nanos
    @Deprecated //TODO
    public static void intersectArraysLimitBatchAsc(long[] a1, long[] a2, int limit) {
        if (limit < 100) {
            limit = 100;
        }
        int count = 0;
        for (int i = 0; i < a1.length; i++) {
            long val = a1[i];
            val = val&a2[i];
            a1[i] = val;
            count += Long.bitCount(val);
            if (count >= limit) {
                break;
            }
        }
    }
    //                    accIds[i/2] = accIds[i/2] | (int)(count << 24);
    //                    tmp.add(allLovers.get(accIds[i] & 0x00FFFFFF));


    public static int getLikesTSAvg(int id, int[] avg) {
        return avg[id];
    }


    public static long addLikesTSAvg(int id, int[] usrIdUnion, int[] avg, int newTs) {
        int usrIdUn =  usrIdUnion[id];
        if (usrIdUn == 0) {
            usrIdUnion[id] = id | (int) (1 << 24);
            avg[id] = newTs;
            return newTs;
        } else {
            int likesNum = usrIdUn & 0x0F000000;
            int usrId = usrIdUn & 0x00FFFFFF;
            int newAvg = (int)(((long)likesNum * avg[usrId] + (long)newTs) / (long)(likesNum + 1));
            avg[usrId] = newAvg;
            usrIdUnion[id] = usrId | (int) (++likesNum << 24);
            return newAvg;
        }
    }


    public static int getBitsCounts(long[] bitMap) {
        int count = 0;
        for (int i = 0; i < bitMap.length; i++) {
            count += Long.bitCount(bitMap[i]);
        }
        return count;
    }

    public static int getBitsCounts(long[] bitMap, int from, int to) {
        int count = 0;
        for (int i = from; i <= to; i++) {
            count += Long.bitCount(bitMap[i]);
        }
        return count;
    }

    //TODO
    public static void intersectArrays(long[] a1, TripleIntArray a2) {
        boolean[] setBits = new boolean[a2.size()];
        for (int i = 0; i < a2.size(); i++) {
            int idx = getFromTriple(a2.data, i);
            setBits[i] = isBitSet(a1, idx);
        }
        clear(a1);
        for (int i = 0; i < a2.size(); i++) {
            int idx = getFromTriple(a2.data, i);
            if (setBits[i]) {
                setBit(a1, idx);
            }
        }
    }

    public static void intersectArrays(long[] a1, int[] a2) {
        boolean[] setBits = new boolean[a2.length];
        for (int i = 0; i < a2.length; i++) {
            int idx = a2[i];
            setBits[i] = isBitSet(a1, idx);
        }
        clear(a1);
        for (int i = 0; i < a2.length; i++) {
            int idx = a2[i];
            if (setBits[i]) {
                setBit(a1, idx);
            }
        }
    }

    //TODO long[] set from pool
    public static PoolEntry transformSetToPoolEntry(int[] accounts, int accNumber) {
        PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[accNumber/64 + (accNumber%64 > 0 ? 1 : 0)];
        for (int i = 0; i < accounts.length; i++) {
            if (accounts[i] != BAD_ACCOUNT_IDX) {
                setBit(set.getObj().bitmap, accounts[i]);
            }
        }
        return set;
    }


    public static Map<Short, long[]> transformSetsToBitMaps(Map<Short, ArrayDataAllocator.IntArray> accounts) {
        Map<Short, long[]> map = new HashMap<>();
        accounts.forEach((key, value) -> {
            long[] set = alloc(MAX_ACCOUNTS);
            for (int i = 0; i < value.array.length; i++) {
                if (value.array[i] != BAD_ACCOUNT_IDX) {
                    setBit(set, value.array[i]);
                }
            }
            map.put(key, set);
        });
        return map;
    }


    public static long[] transformSetToBitMap(long[] set, int[] accounts, int accNumber) {
        for (int i = 0; i < accounts.length; i++) {
            if (accounts[i] != BAD_ACCOUNT_IDX) {
                setBit(set, accounts[i]);
            }
        }
        return set;
    }

    public static long[] fillSet(long[] set, int[] accounts) {
        for (int i = 0; i < accounts.length; i++) {
            if (accounts[i] != BAD_ACCOUNT_IDX) {
                setBit(set, accounts[i]);
            }
        }
        return set;
    }


    static ThreadLocal<long[]> transformTripleTL = new ThreadLocal<long[]>() {

        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }

    };

    //likesForUser.length
    public static long[] transformTripleSetToBitMap(byte[] accounts, int accNumber) {
        long[] set = transformTripleTL.get();
        clear(set);
        for (int i = 0; i < accounts.length / 3; i++) {
            int val = getFromTriple(accounts, i);
            if (val != BAD_ACCOUNT_IDX) {
                setBit(set, val);
            }
        }
        return set;
    }

    public static int fillIntSetFromTriple(byte[] accounts, int[] result) {
        for (int i = 0; i < accounts.length / 3; i++) {
            int val = getFromTriple(accounts, i);
            result[i] = val;
        }
        return accounts.length / 3;
    }


    public static int[] transformTripleSetToInt(byte[] accounts, int accNumber) {
        int[] result = new int[accNumber];
        for (int i = 0; i < accounts.length / 3; i++) {
            int val = getFromTriple(accounts, i);
            result[i] = val;
        }
        return result;
    }


    static ThreadLocal<int[]> transformTL = new ThreadLocal<int[]>() {

        @Override
        protected int[] initialValue() {
            return new int[MAX_ACCOUNTS];
        }

    };

    public static int[] transformBitMapToSet(long[] bitSet, int count) {
        //TODo refactor
        int[] finalSet = new int[count];
        int num = 0;
        for (int i = 0; i < bitSet.length; i++) {
            long chunk = bitSet[i];
            if (chunk == 0) {
                continue;
            }
            for (int j = 0; j < 64; j++) {
                long bitIndex = (long)j;
                long mask = (long)(0x01L << bitIndex);
                if ((chunk&mask) > 0) {
                    finalSet[num++] = i*64 + j;
                }
            }
            if (num == count) {
                break;
            }
        }
        return finalSet;
    }

    //public static int[] finalSet = new int[MAX_ACCOUNTS];

    public static int[] transformBitMapToSetFull(long[] bitSet) {
        //TODo refactor
        int bitsCount = getBitsCounts(bitSet);
        int[] finalSet = new int[bitsCount];
        int num = 0;
        for (int i = 0; i < bitSet.length; i++) {
            long chunk = bitSet[i];
            if (chunk == 0) {
                continue;
            }
            for (int j = 0; j < 64; j++) {
                long bitIndex = (long)j;
                long mask = (long)(0x01L << bitIndex);
                if ((chunk&mask) > 0) {
                    finalSet[num++] = i*64 + j;
                }
            }
        }
        return finalSet;
    }

    public static int getFirst(long[] bitSet) {
        for (int i = 0; i < bitSet.length; i++) {
            long chunk = bitSet[i];

            if (chunk == 0) {
                continue;
            }

            for (int j = 0; j < 64; j++) {
                long bitIndex = (long)j;
                long mask = (long)(0x01L << bitIndex);
                if ((chunk&mask) > 0) {
                    return i*64 + j;
                }
            }
        }
        return -1;
    }

    public static int getLast(long[] bitSet) {
        for (int i = bitSet.length - 1; i >= 0; i--) {
            long chunk = bitSet[i];

            if (chunk == 0) {
                continue;
            }

            for (int j = 63; j >= 0; j--) {
                long bitIndex = (long)j;
                long mask = (long)(0x01L << bitIndex);
                if ((chunk&mask) > 0) {
                    return i*64 + j;
                }
            }
        }
        return -1;
    }


    @Deprecated
    public static int[] transformBitMapToSet(long[] bitSet) {
        if (bitSet == null) {
            return null;
        } else {
            int[] finalSet = transformTL.get();
            DirectMemoryAllocator.clear(finalSet);
            int num = 0;
            for (int i = 0; i < bitSet.length; i++) {
                long chunk = bitSet[i];
                for (int j = 0; j < 64; j++) {
                    long bitIndex = (long)j;
                    long mask = (long)(0x01L << bitIndex);
                    if ((chunk&mask) > 0) {
                        finalSet[num++] = i*64 + j;
                    }
                }
            }
            return finalSet; //realloc(finalSet, num);
        }
    }

    /*
    public static ByteBuffer alloc(int numAccounts) {
        ByteBuffer bb = ByteBuffer.allocateDirect(numAccounts/8 + (numAccounts%8 == 0 ? 0 : 1));
        return bb;
    }

    public static void setBit(ByteBuffer bb, int position) {
        final int dataIndex = position >>> 3;
        byte chunk = bb.get(dataIndex);
        byte bitIndex = (byte)(position%8);
        byte mask = (byte)(0x01 << bitIndex);
        bb.put(dataIndex, (byte)(chunk | mask));
        byte result = bb.get(dataIndex);
    }

    public static boolean isBitSet(ByteBuffer bb, int position) {
        final int dataIndex = position >>> 3;
        byte chunk = bb.get(dataIndex);
        byte bitIndex = (byte)(position%8);
        byte mask = (byte)(0x01 << bitIndex);
        return (byte)(chunk & mask) > 0;
    }
    */

}
