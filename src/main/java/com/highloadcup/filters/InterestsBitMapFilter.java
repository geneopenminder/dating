package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.BitMapHolder;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.accountsInterests;

public class InterestsBitMapFilter extends BaseFilter implements GroupFilter {

    static ThreadLocal<InterestsBitMapFilter> iTL = ThreadLocal.withInitial(InterestsBitMapFilter::new);

    private static final Logger logger = LogManager.getLogger(InterestsBitMapFilter.class);

    public static Map<Short, long[]> interestsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByInterests = new HashMap<Short, Integer>();

    public static Map<Short, ArrayDataAllocator.IntArray> interestsAccounts = new HashMap<Short, ArrayDataAllocator.IntArray>();

    public static long[] nullBitMap = null;
    public static long[] notNullBitMap = null;

    public static int nullCount = 0;
    public static int notNullCount = 0;


    public short[] interests;
    //static Map<Integer, IntArray> accountArrayByInterest = new HashMap<>();

    public InterestsBitMapFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(interestsBitMaps.get(Dictionaries.interests.get(value)), 0, bitset, 0, bitset.length);
    }

    @Override
    public void init() {
        super.init();

        nullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            ArrayDataAllocator.ShortArray interests = accountsInterests[i];
            if (interests != null) {
                notNullCount++;
                DirectMemoryAllocator.setBit(notNullBitMap, i);
                for (short interest : interests.array) {
                    long[] bitMap = interestsBitMaps.get(interest);
                    if (bitMap == null) {
                        bitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                        interestsBitMaps.put(interest, bitMap);
                    }
                    DirectMemoryAllocator.setBit(bitMap, i);
                }
            } else {
                nullCount++;
                DirectMemoryAllocator.setBit(nullBitMap, i);
            }
        }

        interestsBitMaps.forEach((key, value1) -> {
            countByInterests.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            ArrayDataAllocator.ShortArray interests = accountsInterests[i];
            if (interests != null) {
                for (short interest : interests.array) {
                    ArrayDataAllocator.IntArray a = interestsAccounts.get(interest);
                    if (a == null) {
                        a = new ArrayDataAllocator.IntArray();
                        a.array = new int[30000];
                        a.position = 0;
                        interestsAccounts.put(interest, a);
                    } else if (a.position + 1 == a.array.length) {
                        a.array = realloc(a.array, a.array.length + 10000);
                    }
                    a.array[a.position++] = i;
                }
            }
        }
        interestsAccounts.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });
    }

    @Override
    public void processNew(InternalAccount account) {
        ArrayDataAllocator.ShortArray interests = accountsInterests[account.id];
        if (interests != null) {
            notNullCount++;
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);
            for (short interest : interests.array) {
                long[] bitMap = interestsBitMaps.get(interest);
                if (bitMap == null) {
                    bitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    interestsBitMaps.put(interest, bitMap);
                }
                DirectMemoryAllocator.setBit(bitMap, account.id);
                countByInterests.merge(interest, 1, Integer::sum);
            }
        } else {
            nullCount++;
            DirectMemoryAllocator.setBit(nullBitMap, account.id);
        }

    }

    @Override
    public void processUpdate(InternalAccount account) {
        ArrayDataAllocator.ShortArray oldInterests = accountsInterests[account.id];

        if (oldInterests == null) {
            nullCount--;
            notNullCount++;
            DirectMemoryAllocator.unsetBit(nullBitMap, account.id);
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);
        } else {
            //remove old

            for (short interest : oldInterests.array) {
                long[] bitMap = interestsBitMaps.get(interest);
                DirectMemoryAllocator.unsetBit(bitMap, account.id);
                countByInterests.merge(interest, -1, Integer::sum);
            }
        }

        //add new
        ArrayDataAllocator.ShortArray sa = new ArrayDataAllocator.ShortArray();
        sa.array = new short[account.interestsNumber];

        for (int i = 0; i < account.interestsNumber; i++ ) {
            short itrId = Dictionaries.interests.get(Dictionaries.escToUnescapedHashes.get(account.interestsHashes[i]));
            sa.array[i] = itrId;

            long[] bitMap = interestsBitMaps.get(itrId);
            if (bitMap == null) {
                bitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                interestsBitMaps.put(itrId, bitMap);
            }
            DirectMemoryAllocator.setBit(bitMap, account.id);
            countByInterests.merge(itrId, 1, Integer::sum);

        }

        Arrays.sort(sa.array);
        accountsInterests[account.id] = sa;

    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("interests_contains") || predicate.equalsIgnoreCase("interests")) {
            if (interests.length == 0) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = 0;

                for (short i: interests) {
                    if (countByInterests.containsKey(i)) {
                        calculatedPriority += countByInterests.get(i);
                    }
                }
                calculatedPriority = calculatedPriority / interests.length; //TODO

                //Arrays.stream(interests)
                //        .filter(i -> countByInterests.containsKey(i))
                //        .map(i -> countByInterests.get(i)).sum() / interests.length;
            }
        } else if (predicate.equalsIgnoreCase("interests_any")) {
            calculatedPriority = 0;
            for (short i: interests) {
                if (countByInterests.containsKey(i)) {
                    calculatedPriority += countByInterests.get(i);
                }
            }
            //Arrays.stream(interests)
            //        .filter(i -> countByInterests.containsKey(i))
            //        .map(i -> countByInterests.get(i)).sum();
        }
    }

    static ThreadLocal<long[]> bitmapThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    public static void intersectWithFilter(long[] set, int idx) {

        if (accountsInterests[idx] != null && accountsInterests[idx].array != null && accountsInterests[idx].array.length > 0) {

            long[] bitMap = bitmapThreadLocal.get();
            DirectMemoryAllocator.clear(bitMap);

            short[] interests = accountsInterests[idx].array;

            for (short itr : interests) {
                long[] b = interestsBitMaps.get(itr);
                DirectMemoryAllocator.bisectArrays(bitMap, b);
                //int bitCount = DirectMemoryAllocator.getBitsCounts(bitMap);
            }

            DirectMemoryAllocator.intersectArrays(set, bitMap);
        }
    }

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("interests_contains") || predicate.equalsIgnoreCase("interests")) {

            long[] bitMap = bitmapThreadLocal.get();
            DirectMemoryAllocator.clearFF(bitMap);

            //TODO add limit for bisection
            for (short itr:interests) {
                long[] b = interestsBitMaps.get(itr);
                //int bitCountsB = DirectMemoryAllocator.getBitsCounts(b);
                DirectMemoryAllocator.intersectArrays(bitMap, b);
            }

            if (currentSet == null) {
                return bitMap;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, bitMap);
                }
            }
        } else if (predicate.equalsIgnoreCase("interests_any")) {
            long[] bitMap = bitmapThreadLocal.get();
            DirectMemoryAllocator.clear(bitMap);
            //TODO add limit for bisection
            for (short itr:interests) {
                long[] b = interestsBitMaps.get(itr);
                DirectMemoryAllocator.bisectArrays(bitMap, b);
            }

            if (currentSet == null) {
                return bitMap;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitMap, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, bitMap);
                }
            }
        }
        return currentSet;
    }

    @Override
    public boolean checkAccount(int account) {
        long[] filterBitMap = interestsBitMaps.get(Dictionaries.interests.get(value));
        if (filterBitMap == null) {
            return false;
        }
        return DirectMemoryAllocator.isBitSet(filterBitMap, account);
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = interestsBitMaps.get(Dictionaries.interests.get(value));
        if (filterBitMap == null) {
            DirectMemoryAllocator.clear(bitSet);
        } else {
            DirectMemoryAllocator.intersectArrays(bitSet, filterBitMap);
        }
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("interests_contains")) {
            if (currentSet == null) {
                //TODO
                List<Integer> finalSet = new ArrayList<Integer>();
                for (int i = 0; i < accountsInterests.length; i++) {
                    int[] accInterests = accountsInterests[i];
                    if(Arrays.stream(interests)
                            .filter(interest -> Arrays.stream(accInterests).anyMatch(x -> x == interest))
                            .count() == interests.length) {
                        finalSet.add(i);
                    }
                }
                return finalSet.stream().mapToInt(Integer::valueOf).toArray();
            } else {
                for (int i = 0; i < currentSet.length; i++) {
                    //TODO
                    if (currentSet[i] != Integer.MAX_VALUE &&
                            Arrays.stream(accountsInterests[i])
                                    .filter( accI -> Arrays.stream(interests).anyMatch( x -> x == accI))
                                    .count() < interests.length) {
                        currentSet[i] = Integer.MAX_VALUE;
                    }
                }
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("interests_any")) {
            if (currentSet == null) {

                return Arrays.stream(interests).flatMap(i -> {
                    int[] iAcc =  accountArrayByInterest.get(i).array;
                    return Arrays.stream(iAcc);
                }).distinct().toArray(); //TODO
            } else {
                for (int i = 0; i < currentSet.length; i++) {
                    //TODO
                    if (currentSet[i] != Integer.MAX_VALUE &&
                            Arrays.stream(accountsInterests[i])
                                    .filter( accI -> Arrays.stream(interests).anyMatch( x -> x == accI))
                                    .count() == 0) {
                        currentSet[i] = Integer.MAX_VALUE;
                    }
                }
                return currentSet;
            }
        } else {
            return currentSet; //TODO
        }
    }
    */

    @Override
    public InterestsBitMapFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        InterestsBitMapFilter filter = iTL.get();
        filter.fill(predicate, value, limit);

        String[] lVals = value[0].split(",");
        if (lVals.length == 0) {
            filter.interests = new short[0];
        } else {
            short[] newAcc = new short[lVals.length];
            for (int i = 0; i < newAcc.length; i++) {
                newAcc[i] = Dictionaries.interests.get(lVals[i]);
            }
            filter.interests = newAcc;
        }
        return filter;
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty() ||
                value[0].split(",").length == 0) {
            return false;
        }
        if (predicat.equalsIgnoreCase("interests_contains")
                || predicat.equalsIgnoreCase("interests_any")
                || predicat.equalsIgnoreCase("interests")) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> existGroups, boolean singleKey, int direction, int limit) {
        List<GroupItem> groups = new ArrayList<>(50);
        if (existGroups == null) {
            for (Map.Entry<Short, long[]> entry : interestsBitMaps.entrySet()) {
                PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[nullBitMap.length];
                System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, nullBitMap.length);
                if (filteredSet != null) {
                    DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, filteredSet);
                }
                GroupItem g = new GroupItem(Dictionaries.interestsById.get(entry.getKey()), set, direction);
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
                    for (Map.Entry<Short, long[]> entry : interestsBitMaps.entrySet()) {
                        GroupItem newG = new GroupItem(g, direction);
                        newG.addKey(Dictionaries.interestsById.get(entry.getKey()));

                        PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //new long[nullBitMap.length];
                        System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, nullBitMap.length);
                        if (filteredSet != null) {
                            DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, g.set.getObj().bitmap);
                        }
                        groups.add(new GroupItem(Dictionaries.interestsById.get(entry.getKey()), set, direction));
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
