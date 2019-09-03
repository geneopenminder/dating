package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.BitMapHolder;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.sexGlobal;

public class SexFilter extends BaseFilter implements GroupFilter {

    static ThreadLocal<SexFilter> sfThreadLocal = ThreadLocal.withInitial(SexFilter::new);

    //SexFilter
    //public static int[] males = null;
    //public static int[] femails = null;

    public static long[] maleBitMap = null;
    public static long[] femaleBitMap = null;

    public static int totalM = 0;
    public static int totalF = 0;

    public SexFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        if (value.equalsIgnoreCase("1")) {
            System.arraycopy(maleBitMap, 0, bitset, 0, bitset.length);
        } else {
            System.arraycopy(femaleBitMap, 0, bitset, 0, bitset.length);
        }
    }

    @Override
    public void init() {
        super.init();
        //m - 1 : f - 0;

        maleBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        femaleBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            byte s = sexGlobal[i];
            if (s == 1) {
                DirectMemoryAllocator.setBit(maleBitMap, i);
                totalM++;
            } else {
                DirectMemoryAllocator.setBit(femaleBitMap, i);
                totalF++;
            }

        }

        /*
        int[] maleIds = new int[500000];
        AtomicInteger maleIdSeq = new AtomicInteger(0);
        int[] femaleIds = new int[500000];
        AtomicInteger femaleIdSeq = new AtomicInteger(0);

        for (int i = 0; i < sexlobal.length; i++) {
            if (sexlobal[i] == 1) {
                if (maleIds.length == maleIdSeq.get()) {
                    maleIds = realloc(maleIds, maleIds.length + 50000);
                }
                maleIds[maleIdSeq.getAndIncrement()] = i;
            } else {
                if (femaleIds.length == femaleIdSeq.get()) {
                    femaleIds = realloc(femaleIds, femaleIds.length + 50000);
                }
                femaleIds[femaleIdSeq.getAndIncrement()] = i;
            }
        }
        males = realloc(maleIds, maleIdSeq.get() - 1);
        femails = realloc(femaleIds, femaleIdSeq.get() - 1);
        */
    }

    @Override
    public void processNew(InternalAccount a) {
        if (a.sex == 1) {
            DirectMemoryAllocator.setBit(maleBitMap, a.id);
            totalM++;
        } else {
            DirectMemoryAllocator.setBit(femaleBitMap, a.id);
            totalF++;
        }
    }

    @Override
    public void processUpdate(InternalAccount a) {
        byte oldSex = sexGlobal[a.id];

        if (oldSex == 1) {
            DirectMemoryAllocator.unsetBit(maleBitMap, a.id);
            totalM--;
        } else {
            DirectMemoryAllocator.unsetBit(femaleBitMap, a.id);
            totalF--;
        }

        if (a.sex == 1) {
            DirectMemoryAllocator.setBit(maleBitMap, a.id);
            totalM++;
        } else {
            DirectMemoryAllocator.setBit(femaleBitMap, a.id);
            totalF++;
        }

    }

    @Override
    public void calculatePriority() {
        if (value.equalsIgnoreCase("m")) {
            calculatedPriority = totalM;
        } else {
            calculatedPriority = totalF;
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
        final byte sex = value.equalsIgnoreCase("m") ? Byte.valueOf((byte)1) : Byte.valueOf((byte)0);
        if (currentSet == null) {
            if (sex == 1) {
                long[] set = bisTL.get();
                System.arraycopy(maleBitMap, 0, set, 0, set.length);
                return set;
            } else {
                long[] set = bisTL.get();
                System.arraycopy(femaleBitMap, 0, set, 0, set.length);
                return set;
            }
        } else {
            if (isLast) {
                if (sex == 1) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, maleBitMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, femaleBitMap, limit);
                }
            } else {
                if (sex == 1) {
                    DirectMemoryAllocator.intersectArrays(currentSet, maleBitMap);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, femaleBitMap);
                }
            }
        }
        return currentSet;
    }

    @Override
    public void intersect(long[] bitSet) {
        if (value.equalsIgnoreCase("m")) {
            DirectMemoryAllocator.intersectArrays(bitSet, maleBitMap);
        } else {
            DirectMemoryAllocator.intersectArrays(bitSet, femaleBitMap);
        }

    }

    @Override
    public boolean checkAccount(int account) {
        if (value.equalsIgnoreCase("m")) {
            return DirectMemoryAllocator.isBitSet(maleBitMap, account);
        } else {
            return DirectMemoryAllocator.isBitSet(femaleBitMap, account);
        }
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        final int sexlobal = value.equalsIgnoreCase("m") ? 1 : 2;
        if (currentSet == null) {
            if (sexlobal == 1) {
                return copySet(males);
            } else {
                return copySet(femails);
            }
        } else {
            invalidateSetNonEquals(currentSet, ArrayDataAllocator.sexlobal, sexlobal);
        }
        return currentSet;
    }
    */

    @Override
    public SexFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        SexFilter filter = sfThreadLocal.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("sex_eq") || predicat.equalsIgnoreCase("sex")) {
            if (value[0].trim().equalsIgnoreCase("m") ||
                    value[0].trim().equalsIgnoreCase("f")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> existGroups, boolean singleKey, int direction, int limit) {
        List<GroupItem> groups = new ArrayList<>();

        if (existGroups == null) {
            {
                PoolEntry<BitMapHolder> setM = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[maleBitMap.length];
                System.arraycopy(maleBitMap, 0, setM.getObj().bitmap, 0, setM.getObj().bitmap.length);
                if (filteredSet != null) {
                    DirectMemoryAllocator.intersectArrays(setM.getObj().bitmap, filteredSet);
                }
                GroupItem g = new GroupItem("m", setM, direction);
                groups.add(g);
                if (singleKey) {
                    g.calculateCount();
                    FilterObjectsPool.BITMAP_POOL.free(g.set);
                    g.updateSet(null);
                }
                groups.add(g);
            }
            {
                PoolEntry<BitMapHolder> setF = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[maleBitMap.length]; //TODO fill zero
                System.arraycopy(femaleBitMap, 0, setF.getObj().bitmap, 0, setF.getObj().bitmap.length);
                if (filteredSet != null) {
                    DirectMemoryAllocator.intersectArrays(setF.getObj().bitmap, filteredSet);
                }
                GroupItem g = new GroupItem("f", setF, direction);
                if (singleKey) {
                    g.calculateCount();
                    FilterObjectsPool.BITMAP_POOL.free(g.set);
                    g.updateSet(null);
                }
                groups.add(g);
            }
        } else {
            for (GroupItem g : existGroups) {
                try {
                    {
                        GroupItem newG = new GroupItem(g, direction);
                        newG.addKey("m");
                        PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[maleBitMap.length];
                        System.arraycopy(maleBitMap, 0, set.getObj().bitmap, 0, set.getObj().bitmap.length);
                        if (filteredSet != null) {
                            DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, g.set.getObj().bitmap);
                        }
                        newG.updateSet(set);
                        groups.add(newG);
                    }
                    {
                        GroupItem newG = new GroupItem(g, direction);
                        newG.addKey("f");
                        PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[maleBitMap.length];
                        System.arraycopy(femaleBitMap, 0, set.getObj().bitmap, 0, set.getObj().bitmap.length);
                        if (filteredSet != null) {
                            DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, g.set.getObj().bitmap);
                        }
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

}
