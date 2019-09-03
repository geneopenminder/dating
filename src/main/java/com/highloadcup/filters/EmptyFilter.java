package com.highloadcup.filters;

import com.highloadcup.DirectMemoryAllocator;

import static com.highloadcup.ArrayDataAllocator.MAX_ACCOUNTS;
import static com.highloadcup.ArrayDataAllocator.allAccounts;

public class EmptyFilter extends BaseFilter {

    static ThreadLocal<EmptyFilter> efTL = ThreadLocal.withInitial(EmptyFilter::new);

    public EmptyFilter() {
    }

    @Override
    public EmptyFilter clone(String predicate, String[] value, int limit) {
        super.reset();
        EmptyFilter filter = efTL.get();
        filter.fill(predicate, value, limit);
        return filter;
    }

    @Override
    public void calculatePriority() {
        calculatedPriority = Integer.MAX_VALUE; //ArrayDataAllocator.MAX_ACCOUNTS;
    }

    @Override
    public long[] getBitmapFilteredSet(long[] currentSet) {
        if (currentSet == null) {
            return null;
        } else if (isLast) {
            return currentSet;
        } else {
            return currentSet;
        }
    }

    @Override
    public int[] getFilteredSet(int[] currentSet) {
        if (currentSet == null) { //TODO thread local
            return copySet(allAccounts);
        } else {
            return currentSet;
        }
    }

    @Override
    public boolean validatePredicateAndVal(String predicat, String[] value) {
        if (value == null || value.length == 0 || value[0] == null || value[0].isEmpty()) {
            return false;
        }
        if (predicat.equalsIgnoreCase("limit")
                || predicat.equalsIgnoreCase("query_id")) {
            if (!value[0].chars().allMatch( Character::isDigit )) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

}
