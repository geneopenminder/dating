package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.Dictionaries;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.countryGlobal;

public class CountryBitMapFilter extends BaseFilter implements GroupFilter {

    private static final Logger logger = LogManager.getLogger(CountryBitMapFilter.class);

    static ThreadLocal<CountryBitMapFilter> cbfTL = ThreadLocal.withInitial(CountryBitMapFilter::new);

    public static Map<Short, long[]> countryAccountsBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByCountry = new HashMap<Short, Integer>();

    public static long[] nullBitMap = null;
    public static long[] notNullBitMap = null;

    public static int nullAccCount = 0;
    public static int notNullAccCount = 0;

    public short country = Short.MAX_VALUE;

    public CountryBitMapFilter() {
    }

    public CountryBitMapFilter(String predicate, String[] value, int limit) {
        super(predicate, value, limit);
    }

    @Override
    public CountryBitMapFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        CountryBitMapFilter filter = cbfTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void init() {
        super.init();
        nullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        notNullBitMap = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);

        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            short c = countryGlobal[i];
            if (c != Short.MAX_VALUE) {
                notNullAccCount++;
                DirectMemoryAllocator.setBit(notNullBitMap, i);
                long[] acb = countryAccountsBitMaps.get(c);
                if (acb == null) {
                    acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                    countryAccountsBitMaps.put(c, acb);
                }
                DirectMemoryAllocator.setBit(acb, i);
            } else {
                nullAccCount++;
                DirectMemoryAllocator.setBit(nullBitMap, i);
            }
        }

        countryAccountsBitMaps.forEach((key, value1) -> {
            countByCountry.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

    }

    @Override
    public synchronized void processNew(InternalAccount account) {
        short c = account.country;
        if (c != Short.MAX_VALUE) {
            notNullAccCount++;
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);
            long[] acb = countryAccountsBitMaps.get(c);
            if (acb == null) {
                acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                countryAccountsBitMaps.put(c, acb);
            }
            DirectMemoryAllocator.setBit(acb, account.id);
            countByCountry.merge(c, 1, Integer::sum);

        } else {
            nullAccCount++;
            DirectMemoryAllocator.setBit(nullBitMap, account.id);
        }

    }

    @Override
    public synchronized void processUpdate(InternalAccount account) {
        short oldIdx = countryGlobal[account.id];
        short newIdx = account.country;

        if (oldIdx == Short.MAX_VALUE) {
            notNullAccCount++;
            DirectMemoryAllocator.setBit(notNullBitMap, account.id);
            DirectMemoryAllocator.unsetBit(nullBitMap, account.id);
            countByCountry.merge(newIdx, 1, Integer::sum);
        } else {
            countByCountry.merge(oldIdx, -1, Integer::sum);
            countByCountry.merge(newIdx, 1, Integer::sum);

            //remove from old name index
            long[] oldMap = countryAccountsBitMaps.get(oldIdx);
            DirectMemoryAllocator.unsetBit(oldMap, account.id);

        }

        long[] acb = countryAccountsBitMaps.get(newIdx);
        if (acb == null) {
            acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            countryAccountsBitMaps.put(newIdx, acb);
        }
        DirectMemoryAllocator.setBit(acb, account.id);
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

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("country_eq") || predicate.equalsIgnoreCase("country")) {
            Short countryIdx = Dictionaries.countries.get(value);
            if (countryIdx == null) {
                calculatedPriority = 0;
            } else {
                this.country = countryIdx;
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

    static ThreadLocal<long[]> bisTL = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        DirectMemoryAllocator.clear(bisTL.get());
        if (predicate.equalsIgnoreCase("country_eq") || predicate.equalsIgnoreCase("country")) {
            Short cIndex = Dictionaries.countries.get(value);
            if (cIndex == null) {
                return null;
            }
            if (currentSet == null) {
                //TODO copy limit
                long[] bitmap = bisTL.get();
                System.arraycopy(countryAccountsBitMaps.get(cIndex), 0, bitmap, 0, bitmap.length);
                return bitmap;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, countryAccountsBitMaps.get(cIndex), limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, countryAccountsBitMaps.get(cIndex));
                }
            }
        } else if (predicate.equalsIgnoreCase("country_null")) {
            if (value.equalsIgnoreCase("0")) {
                if (currentSet == null) {
                    long[] bitmap = bisTL.get();
                    System.arraycopy(notNullBitMap, 0, bitmap, 0, bitmap.length);
                    return bitmap;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, notNullBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, notNullBitMap);
                    }
                }
            } else {
                if (currentSet == null) {
                    long[] bitmap = bisTL.get();
                    System.arraycopy(nullBitMap, 0, bitmap, 0, bitmap.length);
                    return bitmap;
                } else {
                    if (isLast) {
                        DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, nullBitMap, limit);
                    } else {
                        DirectMemoryAllocator.intersectArrays(currentSet, nullBitMap);
                    }
                }
            }
        }
        return currentSet;
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(countryAccountsBitMaps.get(Dictionaries.countries.get(value)), 0, bitset, 0, bitset.length);
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = countryAccountsBitMaps.get(Dictionaries.countries.get(value));
        if (filterBitMap != null) {
            DirectMemoryAllocator.intersectArrays(bitSet, filterBitMap);
        }
    }

    @Override
    public List<GroupItem> group(long[] filteredSet, List<GroupItem> items, boolean singleKey, int direction, int limit) {
        return null;
    }

}
