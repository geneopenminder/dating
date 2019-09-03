package com.highloadcup.filters.old;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.filters.BaseFilter;
import com.highloadcup.filters.GroupFilter;
import com.highloadcup.filters.GroupItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.highloadcup.ArrayDataAllocator.ACCOUNTS_TOTAL;
import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.countryGlobal;

@Deprecated
public class CountryFilter extends BaseFilter implements GroupFilter {

    static ThreadLocal<CountryFilter> cfTL = ThreadLocal.withInitial(CountryFilter::new);

    public static Map<Short, Integer> countByCountry = new HashMap<Short, Integer>();

    public static long[] nullBitMap = null;
    public static long[] notNullBitMap = null;

    public static int nullAccCount = 0;
    public static int notNullAccCount = 0;


    public static Map<Short, ArrayDataAllocator.IntArray> countryAccounts = new HashMap<>();
    public static int[] nullCoutryAccs = null;
    public static int[] notNullCoutryAccs = null;

    public CountryFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        DirectMemoryAllocator.fillSet(bitset, countryAccounts.get(Dictionaries.countries.get(value)).array);
    }

    @Override
    public CountryFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        CountryFilter filter = cfTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void init() {
        super.init();

        nullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        nullCoutryAccs = new int[200000];
        notNullCoutryAccs = new int[200000];

        final AtomicInteger nullSeq = new AtomicInteger(0);
        final AtomicInteger notNullSeq = new AtomicInteger(0);

        for (int i = 1; i < ACCOUNTS_TOTAL.get() + 1; i++) {
            if (countryGlobal[i] == Short.MAX_VALUE) {
                if (nullCoutryAccs.length == nullSeq.get()) {
                    nullCoutryAccs = realloc(nullCoutryAccs, nullCoutryAccs.length + 50000);
                }
                nullCoutryAccs[nullSeq.getAndIncrement()] = i;
                nullAccCount++;
                DirectMemoryAllocator.setBit(nullBitMap, i);
            } else {
                if (notNullCoutryAccs.length == notNullSeq.get()) {
                    notNullCoutryAccs = realloc(notNullCoutryAccs, notNullCoutryAccs.length + 50000);
                }
                notNullCoutryAccs[notNullSeq.getAndIncrement()] = i;
                notNullAccCount++;
                DirectMemoryAllocator.setBit(notNullBitMap, i);

                short countryIdx = countryGlobal[i];
                ArrayDataAllocator.IntArray a = countryAccounts.get(countryIdx);
                if (a == null) {
                    a = new ArrayDataAllocator.IntArray();
                    a.array = new int[10000];
                    a.position = 0;
                    countryAccounts.put(countryIdx, a);
                } else if (a.position + 1 == a.array.length) {
                    a.array = realloc(a.array, a.array.length + 10);
                }
                a.array[a.position++] = i;
            }
        }

        countryAccounts.values().forEach(arr -> {
            arr.array = realloc(arr.array, arr.position);
        });


        countryAccounts.forEach((key, value1) -> {
            countByCountry.put(key, value1.array.length);
        });
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("country_eq") || predicate.equalsIgnoreCase("country")) {
            Short countryIdx = Dictionaries.countries.get(value);
            if (countryIdx == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = countByCountry.get(countryIdx);
            }
        } else if (predicate.equalsIgnoreCase("country_null")) {
            if (value.equalsIgnoreCase("1")) {
                calculatedPriority = nullAccCount;
            } else {
                calculatedPriority = notNullAccCount;
            }
        }
    }

    @Override
    public boolean validatePredicateAndVal(String predicate, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicate.equalsIgnoreCase("country_eq") || predicate.equalsIgnoreCase("country")) {
            return true;
        } else if (predicate.equalsIgnoreCase("country_null")) {
            String val = value[0].trim().toLowerCase();
            if (val.equalsIgnoreCase("1") || val.equalsIgnoreCase("0")) {
                return true;
            }
        }
        return false;
    }

    //TODO
    public static void updateForAccount(String country, int idx) {
        short oldCountryIdx = countryGlobal[idx]; //
        if (oldCountryIdx != Short.MAX_VALUE) {
            final String oldCountry = Dictionaries.countriesById.get(oldCountryIdx);
            if (!oldCountry.equalsIgnoreCase(country)) {
                short newCountryIdx = 0;
                if (Dictionaries.countries.containsKey(country)) {
                    newCountryIdx = Dictionaries.countries.get(country);
                    ArrayDataAllocator.IntArray countryBits = countryAccounts.get(newCountryIdx);
                    //realloc
                    //DirectMemoryAllocator.setBit(countryBits, idx);
                    countByCountry.put(newCountryIdx, countByCountry.get(newCountryIdx) + 1);
                } else {
                    //newCountryIdx = (short)++ArrayDataAllocator.seq; //TODO
                    Dictionaries.countries.put(country.trim(), newCountryIdx);
                    Dictionaries.countriesById.put(newCountryIdx, country.trim());
                    long[] newCountryBits = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    //countryAccountsBitMaps.put(newCountryIdx, newCountryBits);
                    DirectMemoryAllocator.setBit(newCountryBits, idx);
                    countByCountry.put(newCountryIdx, 1);
                }
                countryGlobal[idx] = newCountryIdx;
                //unset old name bit
                //long[] oldCountryMap = countryAccountsBitMaps.get(oldCountryIdx);
                //DirectMemoryAllocator.unsetBit(oldCountryMap, idx);
                countByCountry.put(oldCountryIdx, countByCountry.get(newCountryIdx) - 1);
                DirectMemoryAllocator.setBit(notNullBitMap, idx);
            }
        }
    }

    static ThreadLocal<long[]> bitmapForIntersectThreadLocal = new ThreadLocal<long[]>() {
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

    public static void intersectWithFilter(long[] set, String value) {
        long[] intersect = bitmapForNewSetThreadLocal.get();
        DirectMemoryAllocator.clear(intersect);
        DirectMemoryAllocator.transformSetToBitMap(intersect,
                countryAccounts.get(Dictionaries.countries.get(value.trim())).array, countryGlobal.length);
        DirectMemoryAllocator.intersectArrays(set, intersect);
    }

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("country_eq") || predicate.equalsIgnoreCase("country")) {
            Short cIndex = Dictionaries.countries.get(value);
            if (cIndex == null) {
                return null;
            }
            int[] a = countryAccounts.get(cIndex).array;
            long[] intersect = currentSet == null ? bitmapForNewSetThreadLocal.get() : bitmapForIntersectThreadLocal.get();
            DirectMemoryAllocator.clear(intersect);
            long[] bitSet = DirectMemoryAllocator.transformSetToBitMap(intersect, a, countryGlobal.length);
            if (currentSet == null) {
                return bitSet;
            } else {
                DirectMemoryAllocator.intersectArrays(currentSet, bitSet);
            }
        } else if (predicate.equalsIgnoreCase("country_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    return copyBitMap(notNullBitMap);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, notNullBitMap);
                }
            } else {
                if (currentSet == null) {
                    return copyBitMap(nullBitMap);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, nullBitMap);
                }
            }
        }
        //boolean isSet = DirectMemoryAllocator.isBitSet(currentSet, (29944));
        return currentSet;
    }

    @Override
    public List<GroupItem> group(long[] set, List<GroupItem> existGroupsm, boolean singleKey, int direction, int limit) {
        return null;
    }
}
