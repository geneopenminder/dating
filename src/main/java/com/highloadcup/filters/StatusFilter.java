package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.BitMapHolder;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.statusGlobal;

public class StatusFilter extends BaseFilter implements GroupFilter {

    static ThreadLocal<StatusFilter> stringBuilderThreadLocal = ThreadLocal.withInitial(StatusFilter::new);

    //StatusFilter
    //public static Map<Integer, IntArray> accountStatuses = new HashMap<>();

    public static long[] freeBitMap = null;
    public static long[] busyBitMap = null;
    public static long[] badBitMap = null;

    public static Map<Byte, long[]> bitmapsByStatuses = new HashMap<Byte, long[]>();

    public static int freeCount = 0;
    public static int busyCount = 0;
    public static int badCount = 0;

    public static Map<Byte, Integer> countsByStatuses = new HashMap<Byte, Integer>();

    public byte filterStatus = 0;

    public StatusFilter() {
    }

    @Override
    public StatusFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        StatusFilter filter = stringBuilderThreadLocal.get();
        filter.fill(predicate, value, limit);
        filter.filterStatus = InternalAccount.getStatus(value[0]);
        return filter;
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(bitmapsByStatuses.get(Short.parseShort(value)), 0, bitset, 0, bitset.length);
    }

    @Override
    public void init() {
        super.init();
        freeBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        busyBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        badBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            int s = ArrayDataAllocator.statusGlobal[i];
            if (s == 1) {
                freeCount++;
                DirectMemoryAllocator.setBit(freeBitMap, i);
            } else if (s == 2) {
                busyCount++;
                DirectMemoryAllocator.setBit(busyBitMap, i);
            } else {
                badCount++;
                DirectMemoryAllocator.setBit(badBitMap, i);
            }
        }

        bitmapsByStatuses.put((byte)1, freeBitMap);
        bitmapsByStatuses.put((byte)2, busyBitMap);
        bitmapsByStatuses.put((byte)3, badBitMap);

        countsByStatuses.put((byte)1, freeCount);
        countsByStatuses.put((byte)2, busyCount);
        countsByStatuses.put((byte)3, badCount);

        /*
        IntArray arr = new IntArray();
        arr.array = status1.stream().mapToInt(Integer::intValue).toArray();
        accountStatuses.put(1, arr);

        arr = new IntArray();
        arr.array = status2.stream().mapToInt(Integer::intValue).toArray();
        accountStatuses.put(2, arr);

        arr = new IntArray();
        arr.array = status3.stream().mapToInt(Integer::intValue).toArray();
        accountStatuses.put(3, arr);
        */
    }

    @Override
    public void processNew(InternalAccount account) {
        byte s = account.status;
        if (s == 1) {
            freeCount++;
            DirectMemoryAllocator.setBit(freeBitMap, account.id);
            countsByStatuses.merge((byte)1, 1, Integer::sum);
        } else if (s == 2) {
            busyCount++;
            DirectMemoryAllocator.setBit(busyBitMap, account.id);
            countsByStatuses.merge((byte)2, 1, Integer::sum);
        } else {
            badCount++;
            DirectMemoryAllocator.setBit(badBitMap, account.id);
            countsByStatuses.merge((byte)3, 1, Integer::sum);
        }
    }

    @Override
    public void processUpdate(InternalAccount account) {
        byte oldStatus = statusGlobal[account.id];
        byte newStatus = account.status;

        if (oldStatus == 1) {
            freeCount--;
            DirectMemoryAllocator.unsetBit(freeBitMap, account.id);
            countsByStatuses.merge((byte)1, -1, Integer::sum);
        } else if (oldStatus == 2) {
            busyCount--;
            DirectMemoryAllocator.unsetBit(busyBitMap, account.id);
            countsByStatuses.merge((byte)2, -1, Integer::sum);
        } else {
            badCount--;
            DirectMemoryAllocator.unsetBit(badBitMap, account.id);
            countsByStatuses.merge((byte)3, -1, Integer::sum);
        }

        if (newStatus == 1) {
            freeCount++;
            DirectMemoryAllocator.setBit(freeBitMap, account.id);
            countsByStatuses.merge((byte)1, 1, Integer::sum);
        } else if (newStatus == 2) {
            busyCount++;
            DirectMemoryAllocator.setBit(busyBitMap, account.id);
            countsByStatuses.merge((byte)2, 1, Integer::sum);
        } else {
            badCount++;
            DirectMemoryAllocator.setBit(badBitMap, account.id);
            countsByStatuses.merge((byte)3, 1, Integer::sum);
        }

    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("status_eq") || predicate.equalsIgnoreCase("status")) {
            calculatedPriority = countsByStatuses.get(filterStatus);
        } else { //status_neq
            calculatedPriority = 0;
            for (Map.Entry<Byte, Integer> e: countsByStatuses.entrySet()) {
                if (e.getKey() != filterStatus) {
                    calculatedPriority += e.getValue();
                }
            }
        }
    }

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("status_eq") || predicate.equalsIgnoreCase("status")) {
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(bitmapsByStatuses.get(filterStatus), 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitmapsByStatuses.get(filterStatus), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, bitmapsByStatuses.get(filterStatus));
                }
            }
        } else if (predicate.equalsIgnoreCase("status_neq")) {

            long[] statusesBitMap = bisTL.get();
            DirectMemoryAllocator.clear(statusesBitMap);

            bitmapsByStatuses.forEach((key, value1) -> {
                if (key != filterStatus) {
                    DirectMemoryAllocator.bisectArrays(statusesBitMap, value1);
                }
            });

            if (DirectMemoryAllocator.getBitsCounts(statusesBitMap) == 0) {
                return null;
            }
            if (currentSet == null) {
                return statusesBitMap;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, statusesBitMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, statusesBitMap);
                }
            }
        }
        return currentSet;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("status_eq")) {
            if (currentSet == null) {
                return copySet(accountStatuses.get(filterStatus).array);
            } else {
                invalidateSetNonEquals(currentSet, statusGlobal, filterStatus);
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("status_neq")) {
            if (currentSet == null) {
                return accountStatuses.keySet().stream()
                        .filter(statusGlobal -> statusGlobal != filterStatus)
                        .flatMapToInt(statusGlobal -> Arrays.stream(accountStatuses.get(statusGlobal).array))
                        .toArray();
            } else {
                invalidateSetEquals(currentSet, statusGlobal, filterStatus);
                return currentSet;
            }
        } else {
            throw new RuntimeException("getFilteredSet FName oooops!");
        }
    }*/

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        filterStatus = InternalAccount.getStatus(value[0]);
        if (filterStatus < 1 || filterStatus > 3) {
            return false;
        }
        if (predicat.equalsIgnoreCase("status_eq")
                || predicat.equalsIgnoreCase("status_neq")
                || predicat.equalsIgnoreCase("status")) {
            return true;
        } else {
            return false;
        }
    }

    public static void updateForAccount(String status, int idx) {
        byte oldStatus = statusGlobal[idx];
        byte newStatus = InternalAccount.getStatus(status);

        if (oldStatus != newStatus) {
            statusGlobal[idx] = newStatus;
            if (newStatus == 1) {
                DirectMemoryAllocator.setBit(freeBitMap, idx);
                DirectMemoryAllocator.unsetBit(busyBitMap, idx);
                DirectMemoryAllocator.unsetBit(badBitMap, idx);
            } else if (newStatus == 2) {
                DirectMemoryAllocator.setBit(busyBitMap, idx);
                DirectMemoryAllocator.unsetBit(freeBitMap, idx);
                DirectMemoryAllocator.unsetBit(badBitMap, idx);
            } else {
                DirectMemoryAllocator.setBit(badBitMap, idx);
                DirectMemoryAllocator.unsetBit(freeBitMap, idx);
                DirectMemoryAllocator.unsetBit(busyBitMap, idx);
            }
            if (oldStatus == 1) {
                freeCount++;
                busyCount--;
                badCount--;
            } else if (oldStatus == 2) {
                busyCount++;
                freeCount--;
                badCount--;
            } else {
                badCount++;
                busyCount--;
                freeCount--;
            }
        }
    }

    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> existGroups, boolean singleKey, int direction, int limit) {
        List<GroupItem> groups = new ArrayList<>(50);

        if (existGroups == null) {
            for (Map.Entry<Byte, long[]> entry : bitmapsByStatuses.entrySet()) {
                PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[freeBitMap.length];
                System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, freeBitMap.length);
                if (filteredSet != null) {
                    DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, filteredSet);
                }
                GroupItem g = new GroupItem(InternalAccount.getStatus(entry.getKey()), set, direction);
                if (singleKey) {
                    g.calculateCount();
                    FilterObjectsPool.BITMAP_POOL.free(g.set);
                    g.updateSet(null);
                }
                groups.add(g);
            }
        } else {
            for (GroupItem g: existGroups) {
                try {
                    for (Map.Entry<Byte, long[]> entry : bitmapsByStatuses.entrySet()) {
                        GroupItem newG = new GroupItem(g, direction);
                        newG.addKey(InternalAccount.getStatus(entry.getKey()));

                        PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[freeBitMap.length];
                        System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, freeBitMap.length);
                        DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, g.set.getObj().bitmap);

                        newG.updateSet(set);
                        groups.add(newG);
                    }
                } finally {
                    FilterObjectsPool.BITMAP_POOL.free(g.set);
                    g.set = null;
                }
            }
        }
        return groups;
    }

    @Override
    public void intersect(long[] bitSet) {
        if (predicate.equalsIgnoreCase("status_eq") || predicate.equalsIgnoreCase("status")) {
            DirectMemoryAllocator.intersectArrays(bitSet, bitmapsByStatuses.get(filterStatus));
        }
    }

}
