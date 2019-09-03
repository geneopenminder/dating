package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.highloadcup.ArrayDataAllocator.*;
import static com.highloadcup.DirectMemoryAllocator.BAD_ACCOUNT_IDX;

public class CityFilter extends BaseFilter implements GroupFilter {

    private static final Logger logger = LogManager.getLogger(CityFilter.class);


    static ThreadLocal<CityFilter> cfTL = ThreadLocal.withInitial(CityFilter::new);

    public static Map<Short, IntArray> cityAccounts = new HashMap<Short, IntArray>();
    public static Map<Short, Integer> countByCity = new HashMap<Short, Integer>();

    public static long[] nullCitiesBitMap = null;
    public static long[] notNullCitiesBitMap = null;

    public static int nullCitiesAccCount = 0;
    public static int notNullCitiesAccCount = 0;

    public short city = Short.MAX_VALUE;

    //CityFilter
    //public static Map<Integer, IntArray> cityAccounts = new HashMap<>();
    //public static int[] nullCities = null;


    public CityFilter() {
    }

    static ThreadLocal<long[]> bmTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        //System.arraycopy(cityAccounts.get(Dictionaries.cities.get(value)), 0, bitset, 0, bitset.length);
    }

    @Override
    public void intersect(long[] bitSet) {
        int[] filterBitMap = cityAccounts.get(Dictionaries.cities.get(value)).array;
        if (filterBitMap == null) {
            DirectMemoryAllocator.clear(bitSet);
        } else {
            long[] bmap = bmTL.get();
            DirectMemoryAllocator.clear(bmap);
            DirectMemoryAllocator.fillSet(bmap, filterBitMap);
            DirectMemoryAllocator.intersectArrays(bitSet, bmap);
        }
    }

    @Override
    public CityFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        CityFilter filter = cfTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void init() {
        super.init();
        nullCitiesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullCitiesBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            short c = cityGlobal[i];

            if (c != Short.MAX_VALUE) {
                notNullCitiesAccCount++;

                IntArray a = cityAccounts.get(c);
                if (a == null) {
                    a = new IntArray();
                    a.array = new int[20000];
                    a.position = 0;
                    cityAccounts.put(c, a);
                } else if (a.position == a.array.length) {
                    a.array = realloc(a.array, a.array.length + 1000);
                }
                a.array[a.position++] = i;

                DirectMemoryAllocator.setBit(notNullCitiesBitMap, i);
            } else {
                nullCitiesAccCount++;
                DirectMemoryAllocator.setBit(nullCitiesBitMap, i);
            }
        }

        cityAccounts.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });

        cityAccounts.forEach((key, value1) -> {
            countByCity.put(key, value1.array.length);
        });

    }

//public static Map<Short, IntArray> cityAccountsTmp = new HashMap<>();

    public static AtomicInteger newCitiesCount = new AtomicInteger(0);

    public static long[] newCities = new long[40000];

    public static void rewriteNew() {
        long rStart = System.nanoTime();
        logger.error("rewrite start - {}", rStart);

        Map<Short, Integer> countByNewCities = new HashMap<>();

        Map<Integer, Long> alreadyUpdated = new HashMap<>();

        for (int i = 0; i < newCitiesCount.get(); i++) {
            long cityAcc = newCities[i];

            int accId = (int)(cityAcc & 0xFFFFFFFFL);
            short cityIdx = (short)(cityAcc >>> 32 & 0xFFFF);

            Long old = alreadyUpdated.put(accId, newCities[i]);

            if (old != null) {
                short oldCityIdx = (short)(old >>> 32 & 0xFFFF);
                countByNewCities.merge(oldCityIdx, -1, Integer::sum);
            }

            countByNewCities.merge(cityIdx, 1, Integer::sum);

        }

        for (Map.Entry e: countByNewCities.entrySet()) {
            short city = (Short)e.getKey();
            IntArray array = cityAccounts.get(city);
            if (array == null) {
                array = new IntArray();
                array.array = new int[countByNewCities.get(city)];
                array.position = 0;
                cityAccounts.put(city, array);
                //logger.error("no accs for city - {}", city);
            } else {
                //logger.error("realloc city {} to {}; old - {}", city, array.array.length + (int)e.getValue(), array.array.length);
                array.position = array.array.length;
                array.array = realloc(array.array, array.array.length + (int)e.getValue() + 1);
            }

            //create group for new city

            GroupBitmapsAggregator.processNewCity(city);
        }

        alreadyUpdated.values().forEach(cityAcc -> {
            int accId = (int)(cityAcc & 0xFFFFFFFFL);
            short cityIdx = (short)(cityAcc >>> 32 & 0xFFFF);

            IntArray array = cityAccounts.get(cityIdx);

            //if (array != null) {
            if (array.array.length > array.position + 1) {
                array.array[array.position++] = accId;
            }
        });

        /*
        for (int i = 0; i < newCitiesCount.get(); i++) {
            long cityAcc = newCities[i];

            int accId = (int)(cityAcc & 0xFFFFFFFFL);
            short cityIdx = (short)(cityAcc >>> 32 & 0xFFFF);

            IntArray array = cityAccounts.get(cityIdx);

            //if (array != null) {
            if (array.array.length > array.position + 1) {
                array.array[array.position++] = accId;
            } else {
                //logger.error("arr length fail for city - {}; length - {}; added - {}",
                //        cityIdx, array.array.length, countByNewCities.get(cityIdx));
            }
            //}
            //logger.error("add account {} for city {}", accId, cityIdx);

        }*/

        newCities = null;
        logger.error("rewrite end - {}", System.nanoTime() - rStart);

    }

    @Override
    public synchronized void processNew(InternalAccount account) {
        short c = account.city;
        if (c != Short.MAX_VALUE) {

            long cityAcc = (long)account.id + ((long)((long)c & 0xFFFFL) << 32L);
            newCities[newCitiesCount.getAndIncrement()] = cityAcc;

            notNullCitiesAccCount++;
            DirectMemoryAllocator.setBit(notNullCitiesBitMap, account.id);

            countByCity.merge(c, 1, Integer::sum);
        } else {
            nullCitiesAccCount++;
            DirectMemoryAllocator.setBit(nullCitiesBitMap, account.id);
        }

    }

    static TreeSet<Integer> alreadyUpdated = new TreeSet<>();

    @Override
    public synchronized void processUpdate(InternalAccount account) {
        short oldIdx = cityGlobal[account.id];
        short newIdx = account.city;

        {
            if (oldIdx == Short.MAX_VALUE) {
                notNullCitiesAccCount++;
                DirectMemoryAllocator.setBit(notNullCitiesBitMap, account.id);
                DirectMemoryAllocator.unsetBit(nullCitiesBitMap, account.id);
                countByCity.merge(newIdx, 1, Integer::sum);
            } else {
                countByCity.merge(oldIdx, -1, Integer::sum);
                countByCity.merge(newIdx, 1, Integer::sum);

                //remove from old name index
                IntArray old = cityAccounts.get(oldIdx);

                int[] oldArr = old.array;

                for (int i = 0; i < oldArr.length; i++) {
                    if (oldArr[i] == account.id) {
                        oldArr[i] = DirectMemoryAllocator.BAD_ACCOUNT_IDX;
                    }
                }
            }
        }

        {
            IntArray a = cityAccounts.get(newIdx);

            boolean inserted = false;

            for (int e = 0; e < a.array.length; e++) {
                if (a.array[e] == BAD_ACCOUNT_IDX) {
                    a.array[e] = account.id;
                    inserted = true;
                    break;
                }
            }

            boolean result = alreadyUpdated.add(account.id);

            if (!result || !inserted || (oldIdx != Short.MAX_VALUE && account.id > ArrayDataAllocator.TOTAL_ACC_LOADED)) {
                long cityAcc = (long)account.id + ((long)((long)newIdx & 0xFFFFL) << 32L);
                newCities[newCitiesCount.getAndIncrement()] = cityAcc;
            }
        }
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("city_eq") || predicate.equalsIgnoreCase("city")) {
            Short cityIdx = Dictionaries.cities.get(value);
            if (cityIdx == null) {
                calculatedPriority = 0;
            } else {
                city = cityIdx;
                calculatedPriority = countByCity.get(cityIdx);
            }
        } else if (predicate.equalsIgnoreCase("city_any")) {
            String[] cities = value.split(",");
            calculatedPriority = 0;

            for (String c: cities) {
                if (Dictionaries.cities.containsKey(c)) {
                    calculatedPriority += countByCity.get(Dictionaries.cities.get(c));
                }
            }
        } else if (predicate.equalsIgnoreCase("city_null")) {
            if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullCitiesAccCount;
            } else {
                calculatedPriority = notNullCitiesAccCount;
            }
        }
    }

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<long[]> bitmapForNewSetThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    static ThreadLocal<long[]> bitmapForIntersectThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("city_eq") || predicate.equalsIgnoreCase("city")) {


            Short cityIndex = Dictionaries.cities.get(value);
            if (cityIndex == null) {
                return null;
            }

            long[] bitSet = currentSet == null ? bitmapForNewSetThreadLocal.get() : bitmapForIntersectThreadLocal.get();
            DirectMemoryAllocator.clear(bitSet);

            DirectMemoryAllocator.fillSet(bitSet, cityAccounts.get(cityIndex).array);

            if (currentSet == null) {
                return bitSet;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, bitSet, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, bitSet);
                }
            }
        } else if (predicate.equalsIgnoreCase("city_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bitmapForNewSetThreadLocal.get();
                    System.arraycopy(notNullCitiesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, notNullCitiesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, notNullCitiesBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] set = bitmapForNewSetThreadLocal.get();
                    System.arraycopy(nullCitiesBitMap, 0, set, 0, set.length);
                    return set;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullCitiesBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullCitiesBitMap);
                    }
                }
            }
        } else if (predicate.equalsIgnoreCase("city_any")) {
            String[] citiesVal = value.split(",");

            Short[] actualCities = new Short[citiesVal.length];
            int length = 0;
            for (int i = 0; i < citiesVal.length; i++) {
                String s = citiesVal[i];
                Short idx = Dictionaries.cities.get(s);
                if (idx != null) {
                    actualCities[i] = idx;
                    length++;
                } else {
                    actualCities[i] = Short.MAX_VALUE;
                }
            }

            if (length == 0) {
                return null;
            }
            //TODO add limit for bisection

            long[] bitMap = bisTL.get();
            DirectMemoryAllocator.clear(bitMap);
            for (int i = 0; i < actualCities.length; i++) {
                if (actualCities[i] != Short.MAX_VALUE) {
                    int[] b = cityAccounts.get(actualCities[i]).array;
                    DirectMemoryAllocator.fillSet(bitMap, b);
                }
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
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("city_eq") || predicat.equalsIgnoreCase("city")
                || predicat.equalsIgnoreCase("city_any")) {
            return true;
        } else if (predicat.equalsIgnoreCase("city_null")) {
            String val = value[0].trim().toLowerCase();
            if (val.equalsIgnoreCase("1") || val.equalsIgnoreCase("0")) {
                return true;
            }
        }
        return false;
    }

    /*
    public static void updateForAccount(String city, int idx) {
        Short oldCityIdx = cityGlobal[idx]; //
        if (oldCityIdx != Short.MAX_VALUE) {
            final String oldCity = Dictionaries.citiesById.get(oldCityIdx);
            if (!oldCity.equalsIgnoreCase(city)) {
                Short newCityIdx = 0;
                if (Dictionaries.cities.containsKey(city)) {
                    newCityIdx = Dictionaries.cities.get(city);
                    long[] cityBits = cityAccounts.get(newCityIdx);
                    DirectMemoryAllocator.setBit(cityBits, idx);
                    countByCity.put(newCityIdx, countByCity.get(newCityIdx) + 1);
                } else {
                    //newCityIdx = (short)++ArrayDataAllocator.seq; //TODO
                    Dictionaries.cities.put(city.trim(), newCityIdx);
                    Dictionaries.citiesById.put(newCityIdx, city.trim());
                    long[] newCityBits = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    cityAccounts.put(newCityIdx, newCityBits);
                    DirectMemoryAllocator.setBit(newCityBits, idx);
                    countByCity.put(newCityIdx, 1);
                }
                cityGlobal[idx] = newCityIdx;
                //unset old name bit
                long[] oldCityMap = cityAccounts.get(oldCityIdx);
                DirectMemoryAllocator.unsetBit(oldCityMap, idx);
                countByCity.put(oldCityIdx, countByCity.get(newCityIdx) - 1);
                DirectMemoryAllocator.setBit(notNullCitiesBitMap, idx);
            }
        }
    }*/

    static ThreadLocal<TreeSet<GroupItem>> treeTL = ThreadLocal.withInitial(new Supplier<TreeSet<GroupItem>>() {
        @Override
        public TreeSet<GroupItem> get() {
            return new TreeSet<GroupItem>();
        }
    });

    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> existGroups, boolean singleKey, int direction, int limit) {

        /*
        final TreeSet<GroupItem> tree = treeTL.get();
        tree.clear();

        List<GroupItem> groups = new ArrayList<>(50);

        GroupItem tmp = new GroupItem(direction);

        if (existGroups == null) {
            for (Map.Entry<Short, long[]> entry : cityAccounts.entrySet()) {
                final String key = Dictionaries.citiesById.get(entry.getKey());

                PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //DirectMemoryAllocator.alloc(cityGlobal.length);
                System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, nullCitiesBitMap.length);
                if (filteredSet != null) {
                    DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, filteredSet);
                }

                Integer bitCount = Integer.valueOf(DirectMemoryAllocator.getBitsCounts(set.getObj().bitmap));
                tmp.count = bitCount;

                if (tree.size() <= limit) {
                    GroupItem g = new GroupItem(key, set, bitCount, direction);
                    if (singleKey) {
                        FilterObjectsPool.BITMAP_POOL.free(g.set);
                        g.updateSet(null);
                    }
                    groups.add(g);
                    tree.add(g);
                } else if (direction < 0) { //-1 biggest
                    tmp.resetKeysOrder();
                    tmp.addKey(key);
                    tmp.count = bitCount;

                    GroupItem lower = tree.first();
                    if (lower.compareTo(tmp) < 0) {
                        GroupItem g = new GroupItem(Dictionaries.citiesById.get(entry.getKey()), set, bitCount, direction);
                        if (singleKey) {
                            FilterObjectsPool.BITMAP_POOL.free(g.set);
                            g.updateSet(null);
                        }
                        groups.add(g);
                        tree.add(g);
                        tree.remove(lower);
                    } else {
                        FilterObjectsPool.BITMAP_POOL.free(set);
                    }
                } else { //lowest
                    tmp.resetKeysOrder();
                    tmp.addKey(key);
                    tmp.count = bitCount;

                    GroupItem higher = tree.last();
                    if (higher.compareTo(tmp) > 0) {
                        GroupItem g = new GroupItem(Dictionaries.citiesById.get(entry.getKey()), set, bitCount, direction);
                        if (singleKey) {
                            FilterObjectsPool.BITMAP_POOL.free(g.set);
                            g.updateSet(null);
                        }
                        groups.add(g);
                        tree.add(g);
                        tree.remove(higher);
                    } else {
                        FilterObjectsPool.BITMAP_POOL.free(set);
                    }
                }
            }
            if (filteredSet != null) {
                PoolEntry<BitMapHolder> nonCitySet = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //DirectMemoryAllocator.alloc(cityGlobal.length);
                System.arraycopy(nullCitiesBitMap, 0, nonCitySet.getObj().bitmap, 0, nullCitiesBitMap.length);
                DirectMemoryAllocator.intersectArrays(nonCitySet.getObj().bitmap, filteredSet);

                GroupItem g = new GroupItem("", nonCitySet, direction);
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
                    {
                        for (Map.Entry<Short, long[]> entry : cityAccounts.entrySet()) {
                            GroupItem newG = new GroupItem(g, direction);
                            newG.addKey(Dictionaries.citiesById.get(entry.getKey()));

                            PoolEntry<BitMapHolder> set = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //DirectMemoryAllocator.alloc(cityGlobal.length);
                            System.arraycopy(entry.getValue(), 0, set.getObj().bitmap, 0, nullCitiesBitMap.length);
                            if (filteredSet != null) {
                                DirectMemoryAllocator.intersectArrays(set.getObj().bitmap, g.set.getObj().bitmap);
                            }
                            newG.updateSet(set);
                            groups.add(newG);
                        }
                    }
                    {
                        if (filteredSet != null) {
                            GroupItem newG = new GroupItem(g, direction);
                            newG.addKey("");
                            PoolEntry<BitMapHolder> nonCitySet = FilterObjectsPool.BITMAP_POOL.take(100, TimeUnit.MILLISECONDS); //DirectMemoryAllocator.alloc(cityGlobal.length);
                            System.arraycopy(nullCitiesBitMap, 0, nonCitySet.getObj().bitmap, 0, nullCitiesBitMap.length);
                            DirectMemoryAllocator.intersectArrays(nonCitySet.getObj().bitmap, g.set.getObj().bitmap);
                            newG.updateSet(nonCitySet);
                            groups.add(newG);
                        }
                    }
                } finally {
                    FilterObjectsPool.BITMAP_POOL.free(g.set);
                    g.set = null;
                }
            }
        }

        return groups;
        */
        return null;
    }

    /*
    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (predicate.equalsIgnoreCase("city_eq")) {
            Integer cityIdx = Dictionaries.cities.get(value);
            if (cityIdx == null) {
                return new int[0];
            } else {
                if (currentSet == null) {
                    return copySet(cityAccounts.get(cityIdx).array);
                } else {
                    invalidateSetNonEquals(currentSet, cityGlobal, cityIdx);
                    return currentSet;
                }

            }
        } else if (predicate.equalsIgnoreCase("city_any")) {
            String[] cities = value.split(",");
            //TODO
            if (currentSet == null) {
                int[] set = Arrays.stream(cities)
                        .filter(Dictionaries.cities::containsKey)
                        .flatMapToInt(cityGlobal -> Arrays.stream(cityAccounts.get(Dictionaries.cities.get(cityGlobal)).array))
                        .toArray();
                return set;
            } else {
                Set<Integer> matchCities = Arrays.stream(cities)
                        .filter(Dictionaries.cities::containsKey)
                        .map(Dictionaries.cities::get)
                        .collect(Collectors.toSet());

                for (int i = 0; i < currentSet.length; i++) {
                    if (currentSet[i] != Integer.MAX_VALUE && !matchCities.contains(cityGlobal[currentSet[i]])) {
                        currentSet[i] = Integer.MAX_VALUE;
                    }
                }
                return currentSet;
            }
        } else if (predicate.equalsIgnoreCase("city_null")) {
            if (value.equalsIgnoreCase("1")) {
                if (currentSet == null) {
                    return copySet(nullCities);
                } else {
                    invalidateSetNonEquals(currentSet, cityGlobal, Integer.MAX_VALUE);
                    return currentSet;
                }
            } else {
                if (currentSet == null) {
                    //TODO
                    return new int[0];
                } else {
                    invalidateSetEquals(currentSet, cityGlobal, Integer.MAX_VALUE);
                    return currentSet;
                }
            }
        } else {
            throw new RuntimeException("getFilteredSet CountryBitMapFilter oooops!");
        }
    }
    */

}
