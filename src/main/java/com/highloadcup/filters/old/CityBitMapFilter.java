package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.BitMapHolder;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.filters.GroupFilter;
import com.highloadcup.filters.GroupItem;
import com.highloadcup.model.InternalAccount;
import com.highloadcup.pool.FilterObjectsPool;
import com.highloadcup.pool.PoolEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.cityGlobal;

@Deprecated
public class CityBitMapFilter extends BaseFilter implements GroupFilter {

    static ThreadLocal<CityBitMapFilter> cfTL = ThreadLocal.withInitial(CityBitMapFilter::new);

    public static Map<Short, long[]> cityAccountsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByCity = new HashMap<Short, Integer>();

    public static long[] nullCitiesBitMap = null;
    public static long[] notNullCitiesBitMap = null;

    public static int nullCitiesAccCount = 0;
    public static int notNullCitiesAccCount = 0;

    //CityBitMapFilter
    //public static Map<Integer, IntArray> cityAccounts = new HashMap<>();
    //public static int[] nullCities = null;


    public CityBitMapFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(cityAccountsBitMaps.get(Dictionaries.cities.get(value)), 0, bitset, 0, bitset.length);
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = cityAccountsBitMaps.get(Dictionaries.cities.get(value));
        if (filterBitMap == null) {
            DirectMemoryAllocator.clear(bitSet);
        } else {
            DirectMemoryAllocator.intersectArrays(bitSet, filterBitMap);
        }
    }

    @Override
    public CityBitMapFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        CityBitMapFilter filter = cfTL.get();
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
                DirectMemoryAllocator.setBit(notNullCitiesBitMap, i);
                long[] acb = cityAccountsBitMaps.get(c);
                if (acb == null) {
                    acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    cityAccountsBitMaps.put(c, acb);
                }
                DirectMemoryAllocator.setBit(acb, i);
            } else {
                nullCitiesAccCount++;
                DirectMemoryAllocator.setBit(nullCitiesBitMap, i);
            }
        }

        cityAccountsBitMaps.forEach((key, value1) -> {
            countByCity.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });


        /*
        nullCities = new int[200000];
        int nullSeq = 0;

        for (int i = 0; i < cityGlobal.length; i++) {
            if (cityGlobal[i] == Integer.MAX_VALUE) {
                if (nullCities.length == nullSeq) {
                    nullCities = realloc(nullCities, nullCities.length + 50000);
                }
                nullCities[nullSeq++] = i;
            }
        }


        List<String> sort = new ArrayList<>(Dictionaries.cities.keySet());

        Collections.sort(sort);


        Dictionaries.cities.forEach((key, value) -> {
            int[] ids = new int[10000];
            AtomicInteger idSeq = new AtomicInteger(0);
            for (int i = 0; i < cityGlobal.length; i++) {
                if (cityGlobal[i] == value) {
                    if (ids.length == idSeq.get()) {
                        ids = realloc(ids, ids.length + 10000);
                    }
                    ids[idSeq.getAndIncrement()] = i;
                }
            }

            IntArray arr = new IntArray();
            arr.array = realloc(ids, idSeq.get() - 1);
            cityAccounts.put(value, arr);
        });

        nullCities = realloc(nullCities, nullSeq - 1);
        */
    }

    @Override
    public void processNew(InternalAccount account) {
        short c = account.city;
        if (c != Short.MAX_VALUE) {
            notNullCitiesAccCount++;
            DirectMemoryAllocator.setBit(notNullCitiesBitMap, account.id);
            long[] acb = cityAccountsBitMaps.get(c);
            if (acb == null) {
                acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                cityAccountsBitMaps.put(c, acb);
            }
            DirectMemoryAllocator.setBit(acb, account.id);
            countByCity.merge(c, 1, Integer::sum);
        } else {
            nullCitiesAccCount++;
            DirectMemoryAllocator.setBit(nullCitiesBitMap, account.id);
        }

    }

    @Override
    public void processUpdate(InternalAccount account) {
        short oldIdx = cityGlobal[account.id];
        short newIdx = account.city;

        if (oldIdx == Short.MAX_VALUE) {
            notNullCitiesAccCount++;
            DirectMemoryAllocator.setBit(notNullCitiesBitMap, account.id);
            DirectMemoryAllocator.unsetBit(nullCitiesBitMap, account.id);
            countByCity.merge(newIdx, 1, Integer::sum);
        } else {
            countByCity.merge(oldIdx, -1, Integer::sum);
            countByCity.merge(newIdx, 1, Integer::sum);

            //remove from old name index
            long[] oldMap = cityAccountsBitMaps.get(oldIdx);
            DirectMemoryAllocator.unsetBit(oldMap, account.id);

        }

        long[] acb = cityAccountsBitMaps.get(newIdx);
        if (acb == null) {
            acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            cityAccountsBitMaps.put(newIdx, acb);
        }
        DirectMemoryAllocator.setBit(acb, account.id);
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("city_eq") || predicate.equalsIgnoreCase("city")) {
            Short cityIdx = Dictionaries.cities.get(value);
            if (cityIdx == null) {
                calculatedPriority = 0;
            } else {
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

    public static void intersectWithFilter(long[] set, String value) {
        long[] interset = cityAccountsBitMaps.get(Dictionaries.cities.get(value.trim()));
        DirectMemoryAllocator.intersectArrays(set, interset);
    }

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
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
            if (currentSet == null) {
                long[] set = bisTL.get();
                System.arraycopy(cityAccountsBitMaps.get(cityIndex), 0, set, 0, set.length);
                return set;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, cityAccountsBitMaps.get(cityIndex), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, cityAccountsBitMaps.get(cityIndex));
                }
            }
        } else if (predicate.equalsIgnoreCase("city_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] set = bisTL.get();
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
                    long[] set = bisTL.get();
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
                    long[] b = cityAccountsBitMaps.get(actualCities[i]);
                    DirectMemoryAllocator.bisectArrays(bitMap, b);
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

    public static void updateForAccount(String city, int idx) {
        Short oldCityIdx = cityGlobal[idx]; //
        if (oldCityIdx != Short.MAX_VALUE) {
            final String oldCity = Dictionaries.citiesById.get(oldCityIdx);
            if (!oldCity.equalsIgnoreCase(city)) {
                Short newCityIdx = 0;
                if (Dictionaries.cities.containsKey(city)) {
                    newCityIdx = Dictionaries.cities.get(city);
                    long[] cityBits = cityAccountsBitMaps.get(newCityIdx);
                    DirectMemoryAllocator.setBit(cityBits, idx);
                    countByCity.put(newCityIdx, countByCity.get(newCityIdx) + 1);
                } else {
                    //newCityIdx = (short)++ArrayDataAllocator.seq; //TODO
                    Dictionaries.cities.put(city.trim(), newCityIdx);
                    Dictionaries.citiesById.put(newCityIdx, city.trim());
                    long[] newCityBits = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    cityAccountsBitMaps.put(newCityIdx, newCityBits);
                    DirectMemoryAllocator.setBit(newCityBits, idx);
                    countByCity.put(newCityIdx, 1);
                }
                cityGlobal[idx] = newCityIdx;
                //unset old name bit
                long[] oldCityMap = cityAccountsBitMaps.get(oldCityIdx);
                DirectMemoryAllocator.unsetBit(oldCityMap, idx);
                countByCity.put(oldCityIdx, countByCity.get(newCityIdx) - 1);
                DirectMemoryAllocator.setBit(notNullCitiesBitMap, idx);
            }
        }
    }

    static ThreadLocal<TreeSet<GroupItem>> treeTL = ThreadLocal.withInitial(new Supplier<TreeSet<GroupItem>>() {
        @Override
        public TreeSet<GroupItem> get() {
            return new TreeSet<GroupItem>();
        }
    });

    @Override
    public List<GroupItem> group(long[] set, List<GroupItem> existGroupsm, boolean singleKey, int direction, int limit) {
        return null;
    }

    /*
    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> existGroups, boolean singleKey, int direction, int limit) {
        final TreeSet<GroupItem> tree = treeTL.get();
        tree.clear();

        List<GroupItem> groups = new ArrayList<>(50);

        GroupItem tmp = new GroupItem(direction);

        if (existGroups == null) {
            for (Map.Entry<Short, long[]> entry : cityAccountsBitMaps.entrySet()) {
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
                        for (Map.Entry<Short, long[]> entry : cityAccountsBitMaps.entrySet()) {
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
    }

*/
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
