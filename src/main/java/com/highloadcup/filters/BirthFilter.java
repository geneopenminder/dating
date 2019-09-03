package com.highloadcup.filters;

import com.highloadcup.ArrayDataAllocator;
import com.highloadcup.DirectMemoryAllocator;
import com.highloadcup.model.InternalAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.time.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.highloadcup.ArrayDataAllocator.*;

public class BirthFilter extends BaseFilter {

    private static final Logger logger = LogManager.getLogger(BirthFilter.class);

    static ThreadLocal<BirthFilter> bfTL = ThreadLocal.withInitial(BirthFilter::new);

    public static Map<Short, long[]> bYearBitMaps = new HashMap<Short, long[]>();
    public static Map<Short, Integer> countByYear = new HashMap<Short, Integer>();


    //public static Map<Integer, IntArray> birthYearsMap = new HashMap<>();
    //public static int[] birthDatesSorted = null;
    //public static byte[] sortedByBirthDatesAccounts = null;

    public int timestamp;

    public BirthFilter() {
    }

    @Override
    public void fillBitSetForGroup(long[] bitset) {
        System.arraycopy(bYearBitMaps.get(Short.parseShort(value)), 0, bitset, 0, bitset.length);
        int bc = DirectMemoryAllocator.getBitsCounts(bitset);
        if (bc > 1000) {
            throw new RuntimeException();
        }
    }

    @Override
    public void intersect(long[] bitSet) {
        long[] filterBitMap = bYearBitMaps.get(Short.parseShort(value));
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
            final int birth = DirectMemoryAllocator.getBirthDateFromFS(i);
            final short year = getYear(birth);

            long[] acb = bYearBitMaps.get(year);
            if (acb == null) {
                acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
                bYearBitMaps.put(year, acb);
            }
            DirectMemoryAllocator.setBit(acb, i);
        }

        bYearBitMaps.forEach((key, value1) -> {
            countByYear.put(key, DirectMemoryAllocator.getBitsCounts(value1));
        });
    }

    @Override
    public synchronized void processNew(InternalAccount a) {
        final int birth = a.birth;
        final short year = getYear(birth);

        long[] acb = bYearBitMaps.get(year);
        if (acb == null) {
            acb = DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
            bYearBitMaps.put(year, acb);
            countByYear.merge(year, 1, Integer::sum);
        } else {
            countByYear.put(year, 1);
        }
        DirectMemoryAllocator.setBit(acb, a.id);
    }

    @Override
    public void processUpdate(InternalAccount a) {

    }

    @Override
    public BaseFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        BirthFilter filter = bfTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    final static TimeZone UTC = TimeZone.getTimeZone("UTC");

    static Calendar cal = Calendar.getInstance();

    public static short getYear(int timestamp) {

        return (short)LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).getYear();
        //Calendar cal = Calendar.getInstance();
        //cal.setTimeZone(UTC);
        //cal.setTime(new Date((long)timestamp * 1000L));
        //return (short)cal.get(Calendar.YEAR);
    }

    @Override
    public void calculatePriority() {
        if (predicate.equalsIgnoreCase("birth_lt")
                || predicate.equalsIgnoreCase("birth_gt")) {
            timestamp = Integer.parseInt(value);
            calculatedPriority = Integer.MAX_VALUE; //TODO
        } else if (predicate.equalsIgnoreCase("birth_year") || predicate.equalsIgnoreCase("birth")) {
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

    static ThreadLocal<long[]> bitmapThreadLocal = new ThreadLocal<long[]>() {
        @Override
        protected long[] initialValue() {
            return DirectMemoryAllocator.alloc(MAX_ACCOUNTS);
        }
    };

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (predicate.equalsIgnoreCase("birth_year") || predicate.equalsIgnoreCase("birth")) {
            short year = Short.parseShort(value);
            long[] yearSet = bYearBitMaps.get(year);
            if (yearSet == null) {
                return null;
            }
            if (currentSet == null) {
                long[] bs = bisTL.get();
                System.arraycopy(yearSet, 0, bs, 0, bs.length);
                return bs;
            } else {
                if (isLast) {
                    DirectMemoryAllocator.intersectArraysLimitDesc(currentSet, yearSet, limit);
                } else {
                    DirectMemoryAllocator.intersectArrays(currentSet, yearSet);
                }
            }
        } else if (predicate.equalsIgnoreCase("birth_lt") || predicate.equalsIgnoreCase("birth_gt")) {
            if (currentSet == null) {
                long[] set = bitmapThreadLocal.get();
                DirectMemoryAllocator.clearFF(set);
                return set;
            }
        }
        return currentSet;
    }

    @Override
    public boolean checkAccount(int account) {
        short year = Short.parseShort(value);
        long[] yearSet = bYearBitMaps.get(year);
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
        if (predicat.equalsIgnoreCase("birth_lt")
                || predicat.equalsIgnoreCase("birth_gt")
                || predicat.equalsIgnoreCase("birth_year")
                || predicat.equalsIgnoreCase("birth")) {
            return true;
        } else {
            return false;
        }
    }

}
