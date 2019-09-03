package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;

public class JoinedFilter extends BaseFilter {

    static ThreadLocal<JoinedFilter> jTL = ThreadLocal.withInitial(JoinedFilter::new);

    public static Map<Short, long[]> yearBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByYear = new HashMap<Short, Integer>();

    public JoinedFilter() {
    }

    @Override
    public JoinedFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        JoinedFilter filter = jTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = yearBitMaps.get(Short.parseShort(value));
        if (filterBitMap == null) {
            DirectMemoryAllocator.clear(bitSet);
        } else {
            DirectMemoryAllocator.intersectArrays(bitSet, filterBitMap);
        }
    }

    @Override
    public void init() {
        super.init();
        for (int i = 1; i < ArrayDataAllocator.ACCOUNTS_TOTAL.get() + 1; i++) {
            final int joined = ArrayDataAllocator.joined[i];
            final short year = getYear(joined);

            byte sex = ArrayDataAllocator.sexGlobal[i];

            long[] acb = yearBitMaps.get(year);
            if (acb == null) {
                acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                yearBitMaps.put(year, acb);
            }
            DirectMemoryAllocator.setBit(acb, i);
        }

        yearBitMaps.forEach((key, value1) -> {
            countByYear.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });

        //clear

        //ArrayDataAllocator.joined = null; //TODO
    }

    static Calendar cal = Calendar.getInstance();

    public static short getYear(long timestamp) {
        cal.setTime(new Date(timestamp * 1000));
        return (short)cal.get(Calendar.YEAR);
    }

    public static short getJoinYear(int joined) {
        return getYear(joined);
    }

    @Override
    public void processNew(InternalAccount account) {
        final int joined = account.joined;
        final short year = getYear(joined);

        long[] acb = yearBitMaps.get(year);
        if (acb == null) {
            acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            yearBitMaps.put(year, acb);
        }
        DirectMemoryAllocator.setBit(acb, account.id);
        countByYear.merge(year, 1, Integer::sum);

    }

    @Override
    public void processUpdate(InternalAccount a) {

    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("joined")) {
            short year = Short.parseShort(value);
            Integer cnt = countByYear.get(year);
            if (cnt == null) {
                calculatedPriority = 0;
            } else {
                calculatedPriority = cnt;
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
        short year = Short.parseShort(value);
        long[] yearSet = yearBitMaps.get(year);
        if (yearSet == null) {
            return null;
        }
        if (currentSet == null) {
            long[] set = bisTL.get();
            System.arraycopy(yearSet,0, set, 0, yearSet.length);
            return set;
        } else {
            if (isLast) {
                DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, yearSet, limit);
            } else {
                DirectMemoryAllocator.intersectArrays(currentSet, yearSet);
            }
        }
        return currentSet;
    }

    @Override
    public boolean checkAccount(int account) {
        short year = Short.parseShort(value);
        long[] yearSet = yearBitMaps.get(year);
        if (yearSet == null) {
            return false;
        }
        return DirectMemoryAllocator.isBitSet(yearSet, account);
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("joined")) {
            return true;
        } else {
            return false;
        }
    }

}
